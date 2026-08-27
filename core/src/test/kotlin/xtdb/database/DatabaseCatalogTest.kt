package xtdb.database

import clojure.lang.Keyword
import kotlinx.coroutines.asCoroutineDispatcher
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.NodeBase
import xtdb.api.error.Conflict
import xtdb.api.error.Fault
import xtdb.api.error.Incorrect
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit.SECONDS

class DatabaseCatalogTest {

    private val ERROR_CODE = Keyword.intern("xtdb.error", "code")

    private fun Conflict.errCode(): String? = (data.valAt(ERROR_CODE) as? Keyword)?.toString()?.removePrefix(":")

    private fun <T : Any> eventually(what: String, f: () -> T?): T {
        val deadline = System.nanoTime() + SECONDS.toNanos(20)
        while (true) {
            f()?.let { return it }
            check(System.nanoTime() < deadline) { "timed out waiting for: $what" }
            Thread.sleep(10)
        }
    }

    @Test
    fun `a database that stops ingesting is put back up`() {
        NodeBase.openBase(openMeterRegistry = false).use { base ->
            DatabaseCatalog.open(base, restartBaseBackoff = 1.milliseconds).use { catalog ->
                catalog.attach("test_db", Database.Config())
                val first = catalog.databaseOrNull("test_db")!!

                first.watchers.notifyError(Fault("storage blip", "xtdb/test-fault"))

                val second = eventually("a replacement database") {
                    catalog.databaseOrNull("test_db")?.takeIf { it !== first }
                }

                assertNull(second.ingestionError, "the replacement is ingesting")
            }
        }
    }

    @Test
    fun `a database waiting to be replaced goes on answering reads`() {
        NodeBase.openBase(openMeterRegistry = false).use { base ->
            // long enough that the wait is observable; under it the name must still resolve
            DatabaseCatalog.open(base, restartBaseBackoff = 3.seconds).use { catalog ->
                catalog.attach("test_db", Database.Config())
                val first = catalog.databaseOrNull("test_db")!!

                first.watchers.notifyError(Fault("storage blip", "xtdb/test-fault"))

                Thread.sleep(500)

                assertSame(first, catalog.databaseOrNull("test_db"), "still the database that stopped")
                assertNotNull(first.ingestionError, "and still carrying why it stopped")
            }
        }
    }

    @Test
    fun `a restarting database stays in the membership record`() {
        NodeBase.openBase(openMeterRegistry = false).use { base ->
            // long enough that the gap between releasing one database and opening the next is
            // observable; a block cut in that gap would otherwise erase the database
            DatabaseCatalog.open(base, restartBaseBackoff = 2.seconds).use { catalog ->
                catalog.attach("test_db", Database.Config())
                val first = catalog.databaseOrNull("test_db")!!

                first.watchers.notifyError(Fault("storage blip", "xtdb/test-fault"))

                eventually("the name to stop resolving") {
                    Unit.takeIf { catalog.databaseOrNull("test_db") == null }
                }

                assertTrue(
                    "test_db" in catalog.serialisedSecondaryDatabases.keys,
                    "a restarting database is still handed to the recorder"
                )
            }
        }
    }

    @Test
    fun `a fault in what was submitted is not restarted`() {
        NodeBase.openBase(openMeterRegistry = false).use { base ->
            DatabaseCatalog.open(base, restartBaseBackoff = 1.milliseconds).use { catalog ->
                catalog.attach("test_db", Database.Config())
                val first = catalog.databaseOrNull("test_db")!!

                first.watchers.notifyError(Incorrect("poison record", "xtdb/test-incorrect"))

                Thread.sleep(500)

                assertSame(first, catalog.databaseOrNull("test_db"), "the same database is still there")
                assertNotNull(first.ingestionError, "and it is still failed")
            }
        }
    }

    @Test
    fun `reattach during detach returns transient conflict (#5613)`() {
        NodeBase.openBase(openMeterRegistry = false).use { base ->
            // Pin the closer to a single-thread dispatcher whose one thread we hold with `gate`, so the
            // detaching database is held mid-teardown for as long as we need to observe the conflict.
            // The teardown otherwise completes on a background thread and would race the re-attach to
            // win the observation.
            val gate = CountDownLatch(1)
            val closerExecutor = Executors.newSingleThreadExecutor()
            try {
                closerExecutor.execute {
                    try { gate.await() } catch (e: InterruptedException) { Thread.currentThread().interrupt() }
                }

                DatabaseCatalog.open(base, closerExecutor.asCoroutineDispatcher()).use { catalog ->
                    catalog.attach("test_db", Database.Config())
                    try {
                        // The teardown coroutine is queued behind the gate, so the entry stays Detaching
                        // and the re-attach sees the transient conflict.
                        catalog.detach("test_db")

                        val ex = assertThrows<Conflict> {
                            catalog.attach("test_db", Database.Config())
                        }
                        assertEquals("xtdb/db-being-detached", ex.errCode())
                    } finally {
                        // Release before `use` closes the catalog — close() joins the closer's children.
                        gate.countDown()
                    }

                    // With teardown released, the name frees up and re-attach eventually succeeds.
                    val deadline = System.nanoTime() + SECONDS.toNanos(10)
                    while (true) {
                        try {
                            catalog.attach("test_db", Database.Config()); break
                        } catch (e: Conflict) {
                            check(System.nanoTime() < deadline) { "detach did not complete within 10s" }
                            Thread.sleep(10)
                        }
                    }
                }
            } finally {
                gate.countDown()
                closerExecutor.shutdownNow()
            }
        }
    }
}
