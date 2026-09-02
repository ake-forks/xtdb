package xtdb.database

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import xtdb.NodeBase
import xtdb.compactor.Compactor
import xtdb.database.proto.DatabaseConfig
import xtdb.diagnostics.TeardownStall
import xtdb.api.error.Anomaly
import xtdb.api.error.Conflict
import xtdb.api.error.Fault
import xtdb.api.error.Incorrect
import xtdb.api.error.NotFound
import xtdb.api.log.Watchers
import xtdb.api.DatabaseName
import xtdb.util.closeAll
import xtdb.util.closeOnCatch
import xtdb.util.debug
import xtdb.util.error
import xtdb.util.logger
import xtdb.util.warn
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

private val LOG = DatabaseCatalog::class.logger

// Sized against a dependency outage rather than a poison record: eight attempts spans roughly ten
// minutes, which covers a storage or log blip without an operator. A fault that recurs identically
// is not retried at all — see `restartable` — so the count only ever bounds the other kind.
private const val MAX_RESTART_ATTEMPTS = 8
private val RESTART_BASE_BACKOFF = 5.seconds
private val RESTART_MAX_BACKOFF = 5.minutes

class DatabaseCatalog @JvmOverloads constructor(
    private val base: NodeBase,
    private val compactor: Compactor,
    closerDispatcher: CoroutineDispatcher = Dispatchers.IO,
    private val restartBaseBackoff: Duration = RESTART_BASE_BACKOFF,
) : Database.Catalog, AutoCloseable {

    private fun backoff(attempts: Int): Duration =
        (restartBaseBackoff * (1 shl (attempts - 1).coerceIn(0, 8))).coerceAtMost(RESTART_MAX_BACKOFF)

    // A block records the whole secondary list and replaces the previous one, so an entry left out
    // of `serialisedSecondaryDatabases` while a block is cut is erased rather than skipped.
    private sealed interface Entry {
        val config: Database.Config

        /**
         * An entry with a database behind it to answer a caller. Ingestion having stopped does not
         * take that away — a database keeps its basis and its read path — so the states that differ
         * only in what is being done about the ingestion all resolve.
         */
        sealed interface Resolvable : Entry {
            val db: Database
            override val config get() = db.config
        }

        class Open(override val db: Database) : Resolvable

        /** Ingestion has stopped, and a replacement is due. */
        class Recovering(override val db: Database) : Resolvable

        /** Ingestion has stopped, and this node will do nothing further about it. */
        class Stopped(override val db: Database) : Resolvable

        class Skipped(override val config: Database.Config) : Entry

        /**
         * Being replaced: the outgoing database has been released and its replacement is being
         * opened. The entry holds none of its own — the supervisor is the sole owner and closer of
         * what it released, so nothing else may close it — but it keeps the name occupied, which is
         * what stops a re-admission opening a second database over the same log and storage.
         *
         * Still a member: a block cut while it is out of the set erases the database.
         */
        class Restarting(override val config: Database.Config) : Entry

        /** Recovery ended with nothing behind the name. A member still, so an operator finds it. */
        class Stranded(override val config: Database.Config) : Entry

        /**
         * Removed, and being torn down. The variant says who owns that teardown, because two owners
         * would double-close: [ByCatalog] where [detach] took a live database, [ByRestart] where it
         * arrived mid-restart and the supervisor is already releasing one.
         */
        sealed interface Detaching : Entry {
            class ByCatalog(val db: Database) : Detaching {
                override val config get() = db.config
            }

            class ByRestart(override val config: Database.Config) : Detaching
        }
    }

    private val entries = ConcurrentHashMap<DatabaseName, Entry>()

    // Parent of every database's job tree. A SupervisorJob so one database's failure is contained
    // here (on the common parent) rather than cancelling its siblings; node shutdown cancels this
    // once to stop every database's indexing/compaction in one go.
    private val dbJob = SupervisorJob()
    private val dbScope = CoroutineScope(dbJob)

    // Detaching databases tear down off the caller's thread on this scope, and keep their entry
    // until that completes — see #5613. Nested under `dbJob` so node shutdown's single cancel covers it.
    private val closerJob = SupervisorJob(dbJob)
    private val closerScope = CoroutineScope(closerJob + closerDispatcher)

    // Restarts are a sibling of `closerJob`, not a child: `close` joins that job's children before
    // cancelling, and joining a restart would stall shutdown for the length of a backoff. Cancelling
    // one is safe — the outgoing database is released under `NonCancellable`, and a replacement that
    // has been opened is closed on the way out rather than left behind.
    private val restartJob = SupervisorJob(dbJob)
    private val restartScope = CoroutineScope(restartJob + closerDispatcher)

    /** How many supervisors are still watching a database — one per name this node expects to keep. */
    internal val liveSupervisors: Int get() = restartJob.children.count()

    override val databaseNames: Collection<DatabaseName>
        get() = entries.entries.asSequence().filter { it.value is Entry.Resolvable }.map { it.key }.toSet()

    override val txScoped = false

    override fun databaseOrNull(dbName: DatabaseName): Database? = (entries[dbName] as? Entry.Resolvable)?.db

    override val abandonedDatabases: Map<DatabaseName, Database.Config>
        get() = entries.entries
            .filter { it.value is Entry.Stopped || it.value is Entry.Stranded }
            .associate { (dbName, entry) -> dbName to entry.config }

    override val serialisedSecondaryDatabases: Map<DatabaseName, DatabaseConfig>
        get() = entries.entries
            .filter { it.key != "xtdb" && it.value !is Entry.Detaching }
            .associate { (dbName, entry) -> dbName to entry.config.serializedConfig }

    private val skipDbs: Set<String> get() = base.config.skipDbs

    override fun checkCanAttach(dbName: DatabaseName, config: Database.Config) {
        when (entries[dbName]) {
            is Entry.Detaching -> throw Conflict(
                "Database is still being detached — retry once the previous detach has completed",
                "xtdb/db-being-detached",
                mapOf("db-name" to dbName)
            )

            is Entry.Resolvable, is Entry.Skipped, is Entry.Restarting, is Entry.Stranded ->
                throw Conflict("Database already exists", "xtdb/db-exists", mapOf("db-name" to dbName))

            null -> {}
        }

        config.checkValid(dbName)
    }

    override fun checkCanDetach(dbName: DatabaseName) {
        if (dbName == "xtdb")
            throw Incorrect("Cannot detach the primary 'xtdb' database", "xtdb/cannot-detach-primary", mapOf("db-name" to dbName))

        when (entries[dbName]) {
            // a restart is revoked and a stranded name goes straight out, so both are removable
            is Entry.Resolvable, is Entry.Skipped, is Entry.Restarting, is Entry.Stranded -> {}

            is Entry.Detaching, null ->
                throw NotFound("Database does not exist", "xtdb/no-such-db", mapOf("db-name" to dbName))
        }
    }

    override fun attach(dbName: DatabaseName, config: Database.Config?) {
        val dbConfig = config ?: Database.Config()
        checkCanAttach(dbName, dbConfig)

        if (dbName in skipDbs) {
            LOG.warn { "Skipping database '$dbName' (XTDB_SKIP_DBS) — database is dormant. Remove from XTDB_SKIP_DBS and restart to re-enable, or DETACH DATABASE to remove permanently." }
            entries[dbName] = Entry.Skipped(dbConfig)
            return
        }

        val readOnlyConfig = if (base.config.readOnlyDatabases) dbConfig.mode(Database.Mode.READ_ONLY) else dbConfig

        val db = try {
            Database.open(base, dbName, readOnlyConfig, compactor, dbScope, dbCatalogFor(dbName))
        } catch (t: Throwable) {
            LOG.debug { "Failed to open database: db-name=$dbName, exception=${t.javaClass}, message=${t.message}" }
            t.cause?.let { LOG.debug { "Cause: class=${it.javaClass}, message=${it.message}" } }
            if (t is IllegalStateException) throw t
            throw Incorrect("Failed to open database", "xtdb.db-catalog/invalid-db-config", mapOf("db-name" to dbName), t)
        }

        db.closeOnCatch {
            val open = Entry.Open(db)

            // putIfAbsent rather than a bare put: `Database.open` above is I/O, so two attaches of one
            // name can both reach here and the loser's database would be overwritten — leaving a live
            // allocator, live coroutines and a registered group subscription that nothing will close.
            if (entries.putIfAbsent(dbName, open) != null)
                throw Conflict("Database already exists", "xtdb/db-exists", mapOf("db-name" to dbName))

            // Every way a database becomes live funnels through here, so this is the one place a
            // supervisor is armed. The primary gets one too, and `restartable` refuses it: what the
            // supervisor does for it is record that nothing more is coming, which is what takes the
            // node out of service. Restarting it is what must not happen — its log processor applies
            // the cluster's membership instructions, and those mutate this catalog, which no restart
            // of a single database rewinds, so a replayed instruction meets its own earlier effect
            // and resolves a second time; and it is the resolution rather than the instruction that
            // every node applies.
            supervise(dbName, open)
        }
    }

    private fun dbCatalogFor(dbName: DatabaseName) = this.takeIf { dbName == "xtdb" }

    /**
     * Whether a database that has just stopped ingesting is worth re-opening.
     *
     * A caller fault is in what was submitted, so it recurs identically however many times the
     * database is replaced; anything else may have cleared by the time we look again.
     */
    private fun restartable(dbName: DatabaseName, cause: Throwable?, attempts: Int): Boolean {
        if (dbName == "xtdb") {
            LOG.error(cause ?: Exception("no cause recorded")) {
                "The primary database stopped ingesting; this node has nothing further to do."
            }
            return false
        }

        if (cause is Anomaly.Caller) {
            LOG.error(cause) { "Database '$dbName' stopped ingesting on a fault that would recur — not restarting." }
            return false
        }

        if (attempts > MAX_RESTART_ATTEMPTS) {
            LOG.error(cause ?: Exception("no cause recorded")) {
                "Database '$dbName' has failed $MAX_RESTART_ATTEMPTS times without getting any further — not restarting."
            }
            return false
        }

        return true
    }

    /** Releases a database, on the path where this catalog owns doing so. */
    private suspend fun release(dbName: DatabaseName, db: Database) =
        // once teardown starts it must finish: a cancel caught mid-way would leave the state behind
        withContext(NonCancellable) {
            try {
                db.cancelAndJoin()
                db.close()
            } catch (t: Throwable) {
                LOG.error(t) { "Failed to close database '$dbName'" }
            }
        }

    /** Clears an entry a detach left waiting on this supervisor to finish. */
    private fun dropIfWaiting(dbName: DatabaseName) {
        (entries[dbName] as? Entry.Detaching.ByRestart)?.let { entries.remove(dbName, it) }
    }

    /**
     * Puts a database back up for as long as it keeps stopping in ways that might clear.
     *
     * Every write here is a compare-and-set against the entry this supervisor installed, so losing
     * one means a detach got there first: it then releases whatever it holds and stops, rather than
     * installing a database under a name the cluster believes is gone. The entries are compared by
     * identity — `Database.equals` is by name, so it cannot tell a replacement from its predecessor.
     *
     * The count bounds a run of failures rather than a database's lifetime: one that got further
     * than it did last time has recovered from whatever stopped it before, and starts again at one.
     * That position is a tx-id — see [Watchers.Failure].
     */
    /** Whichever comes first: this database failing, or someone else tearing it down. Null for the latter. */
    private suspend fun awaitFailureOrTeardown(db: Database): Watchers.Failure? = coroutineScope {
        val failure = async { db.awaitFailure() }
        val teardown = async { db.awaitTeardown() }

        select {
            failure.onAwait { teardown.cancel(); it }
            teardown.onAwait { failure.cancel(); null }
        }
    }

    private fun supervise(dbName: DatabaseName, installed: Entry.Open) = restartScope.launch {
        var current: Entry.Resolvable = installed
        var attempts = 0
        var lastFailedAt = Long.MIN_VALUE

        while (true) {
            val db = current.db

            // Whoever tears this database down is not obliged to fail it -- a clean teardown never
            // reaches the watchers -- so waiting on the failure alone parks here for the rest of the
            // node's life, holding a database that has already been closed and replaced by nobody.
            val failure = awaitFailureOrTeardown(db) ?: return@launch

            attempts = if (failure.latestTxId > lastFailedAt) 1 else attempts + 1
            lastFailedAt = failure.latestTxId

            if (!restartable(dbName, failure.exception.cause, attempts)) {
                // still answering reads, and now saying that nothing more is coming
                entries.replace(dbName, current, Entry.Stopped(db))
                return@launch
            }

            // The wait happens with the database still behind the name, answering reads at the basis it
            // reached, so what a caller loses is the replacement's open rather than the wait for one.
            // A detach arriving now takes that database itself, and the claim below then fails.
            val recovering = Entry.Recovering(db)
            if (!entries.replace(dbName, current, recovering)) return@launch

            delay(backoff(attempts))

            val claim = Entry.Restarting(db.config)
            if (!entries.replace(dbName, recovering, claim)) {
                dropIfWaiting(dbName)
                return@launch
            }

            release(dbName, db)

            var fresh: Database? = null

            while (fresh == null) {
                // Cheap check before an open we would only close again; the compare-and-set below is
                // what actually makes the race safe.
                if (entries[dbName] !== claim) {
                    dropIfWaiting(dbName)
                    return@launch
                }

                fresh = try {
                    Database.open(base, dbName, claim.config, compactor, dbScope, dbCatalogFor(dbName))
                } catch (t: Throwable) {
                    LOG.warn(t, "Failed to re-open database '$dbName'")
                    attempts++

                    // classified on the open's own fault, not the one that stopped ingestion: a
                    // config the node cannot open recurs identically, however long we wait
                    if (!restartable(dbName, t, attempts)) {
                        entries.replace(dbName, claim, Entry.Stranded(claim.config))
                        dropIfWaiting(dbName)
                        return@launch
                    }

                    delay(backoff(attempts))
                    null
                }
            }

            // NonCancellable so a cancel between opening and installing cannot strand the replacement.
            val landed = withContext(NonCancellable) {
                Entry.Open(fresh).takeIf { entries.replace(dbName, claim, it) }
            }

            if (landed == null) {
                release(dbName, fresh)
                dropIfWaiting(dbName)
                return@launch
            }

            current = landed
        }
    }

    override fun detach(dbName: DatabaseName) {
        checkCanDetach(dbName)

        fun noSuchDb(): Nothing =
            throw NotFound("Database does not exist", "xtdb/no-such-db", mapOf("db-name" to dbName))

        val resolvable = when (val entry = entries[dbName]) {
            // nothing runs behind either, so the record goes straight out
            is Entry.Skipped, is Entry.Stranded -> {
                if (!entries.remove(dbName, entry)) noSuchDb()
                return
            }

            // The supervisor is already releasing this one. Revoking its claim is the whole of the
            // work here: its next compare-and-set fails, and it closes what it holds and drops the
            // entry. Tearing down here as well would be a second owner of one teardown.
            is Entry.Restarting -> {
                if (!entries.replace(dbName, entry, Entry.Detaching.ByRestart(entry.config))) noSuchDb()
                return
            }

            is Entry.Resolvable -> entry

            is Entry.Detaching, null -> noSuchDb()
        }

        // Close off the persister's stack — see #5613. `cancelAndJoin` suspends rather than parking a
        // thread in `runBlocking`, so the detach can't deadlock against another thread-parking
        // teardown on a constrained dispatcher.
        val detaching = Entry.Detaching.ByCatalog(resolvable.db)
        if (!entries.replace(dbName, resolvable, detaching)) noSuchDb()

        closerScope.launch {
            // NonCancellable: once teardown starts it must run to completion. Node shutdown cancels
            // `dbJob` (this coroutine's ancestor); without the shield a detach caught mid-cancelAndJoin
            // would skip `db.close()` yet still drop the entry — leaking its state.
            withContext(NonCancellable) {
                try {
                    resolvable.db.cancelAndJoin()
                    resolvable.db.close()
                } catch (t: Throwable) {
                    LOG.error(t) { "Failed to close detaching database '$dbName'" }
                } finally {
                    entries.remove(dbName, detaching)
                }
            }
        }
    }

    override fun close() {
        val stalled = TeardownStall.runBounded("DatabaseCatalog.close") {
            // Let in-flight detaches finish their own teardown before we cancel the tree.
            closerJob.children.toList().forEach { it.join() }
            dbJob.cancelAndJoin()
        }

        if (stalled) {
            // Skip Phase 2: freeing an allocator while the wedged tree is still live is a
            // use-after-free. Leak it and fail loud (runBounded already dumped).
            throw Fault("database catalog did not shut down in time", "xtdb/db-close-timeout")
        }

        entries.values.mapNotNull {
            when (it) {
                is Entry.Resolvable -> it.db
                is Entry.Detaching.ByCatalog -> it.db
                // the supervisor holds and closes what it released; `dbJob.cancelAndJoin` waited for it
                is Entry.Detaching.ByRestart, is Entry.Restarting -> null
                is Entry.Skipped, is Entry.Stranded -> null
            }
        }.closeAll()
    }

    companion object {
        @JvmStatic
        @JvmOverloads
        fun open(
            base: NodeBase,
            closerDispatcher: CoroutineDispatcher = Dispatchers.IO,
            restartBaseBackoff: Duration = RESTART_BASE_BACKOFF,
        ): DatabaseCatalog {
            val catalog = DatabaseCatalog(base, base.compactor, closerDispatcher, restartBaseBackoff)

            catalog.closeOnCatch {
                val conf = base.config
                val xtdbDbConfig = Database.Config()
                    .log(conf.log)
                    .storage(conf.storage)
                    .let { if (conf.readOnlyDatabases) it.mode(Database.Mode.READ_ONLY) else it }

                catalog.attach("xtdb", xtdbDbConfig)

                val secondaryDbs = catalog.primary.tableCatalog.secondaryDatabases
                for ((dbName, dbProtoConfig) in secondaryDbs) {
                    if (dbName == "xtdb") continue
                    val dbConfig = Database.Config.fromProto(dbProtoConfig)
                    catalog.attach(dbName, dbConfig)
                }
            }

            return catalog
        }
    }
}
