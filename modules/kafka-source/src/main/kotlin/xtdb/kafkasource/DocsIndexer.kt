package xtdb.kafkasource

import kotlinx.coroutines.CancellationException
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.subclass
import org.apache.kafka.clients.consumer.ConsumerRecord
import xtdb.error.Fault
import xtdb.error.Incorrect
import xtdb.indexer.OpenTx
import xtdb.indexer.TxIndexer
import xtdb.indexer.TxIndexer.TxResult
import xtdb.kafkasource.proto.DocsIndexerConfig
import xtdb.kafkasource.proto.docsIndexerConfig
import xtdb.kafkasource.proto.kafkaTopicSourceToken
import xtdb.table.TableRef
import xtdb.time.InstantUtil.asMicros
import xtdb.util.asIid
import java.nio.ByteBuffer
import java.time.Instant
import com.google.protobuf.Any as ProtoAny

private const val PROTO_TAG_PREFIX = "proto.xtdb.com"
private const val DEFAULT_SCHEMA = "public"

class DocsIndexer(
    private val table: TableRef,
) : RecordIndexer<Any?, Any?> {

    @Serializable
    @SerialName("!Docs")
    data class Factory(
        val table: String,
    ) : RecordIndexer.Factory<Any?, Any?> {

        override fun open(dbName: String): RecordIndexer<Any?, Any?> {
            val (schema, tableName) = parseTable(table)
            return DocsIndexer(TableRef(dbName, schema, tableName))
        }

        override fun toProto(): ProtoAny =
            ProtoAny.pack(docsIndexerConfig { table = this@Factory.table }, PROTO_TAG_PREFIX)

        class Registration : RecordIndexer.Registration {
            override val protoTag: String
                get() = "$PROTO_TAG_PREFIX/xtdb.kafkasource.proto.DocsIndexerConfig"

            override fun fromProto(msg: ProtoAny): RecordIndexer.Factory<*, *> {
                val config = msg.unpack(DocsIndexerConfig::class.java)
                return Factory(table = config.table)
            }

            override fun registerSerde(builder: PolymorphicModuleBuilder<RecordIndexer.Factory<*, *>>) {
                builder.subclass(Factory::class)
            }
        }
    }

    override suspend fun indexRecords(
        records: List<ConsumerRecord<Any?, Any?>>,
        txIndexer: TxIndexer,
    ) {
        for (rec in records) {
            val token = kafkaTopicSourceToken { offset = rec.offset() }.toByteArray()
            val systemTime = Instant.ofEpochMilli(rec.timestamp())
            val coords = coordsFor(rec)

            txIndexer.indexTx(token, systemTime = systemTime) { openTx ->
                try {
                    writeRecord(openTx, rec)
                    TxResult.Committed()
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Throwable) {
                    TxResult.Aborted(wrapWithCoords(e, rec, coords), userMetadata = coords)
                }
            }
        }
    }

    private fun coordsFor(rec: ConsumerRecord<*, *>): Map<String, Any?> =
        mapOf("topic" to rec.topic(), "partition" to rec.partition(), "offset" to rec.offset())

    private fun wrapWithCoords(cause: Throwable, rec: ConsumerRecord<*, *>, coords: Map<String, Any?>): Throwable =
        Fault(
            "!Docs indexer failed at ${rec.topic()}-${rec.partition()} offset ${rec.offset()}: ${cause.message}",
            "xtdb.kafka-source/docs-indexer-failed",
            coords,
            cause = cause,
        )

    private fun writeRecord(openTx: OpenTx, rec: ConsumerRecord<Any?, Any?>) {
        val openTxTable = openTx.table(table)
        val value = rec.value()

        if (value == null) {
            val id = rec.key() ?: throw Incorrect(
                "tombstone has no key — can't resolve _id",
                "xtdb.kafka-source/docs-no-id-on-tombstone",
                mapOf("topic" to rec.topic(), "partition" to rec.partition(), "offset" to rec.offset()),
            )
            openTxTable.logDelete(
                ByteBuffer.wrap(id.asIid),
                openTx.systemFrom,
                Long.MAX_VALUE,
            )
            return
        }

        val docMap = (value as? Map<*, *>)?.toMutableMap()
            ?: throw Incorrect(
                "expected map-shaped value, got ${value.javaClass.name} — !Docs needs a map per record",
                "xtdb.kafka-source/docs-non-map-value",
                mapOf(
                    "topic" to rec.topic(),
                    "partition" to rec.partition(),
                    "offset" to rec.offset(),
                    "valueType" to value.javaClass.name,
                ),
            )

        val id = docMap["_id"] ?: rec.key()?.also { docMap["_id"] = it }
            ?: throw Incorrect(
                "no _id on doc and no record key — can't resolve _id",
                "xtdb.kafka-source/docs-no-id",
                mapOf("topic" to rec.topic(), "partition" to rec.partition(), "offset" to rec.offset()),
            )

        val explicitValidFrom = (docMap.remove("_valid_from") as? Instant)?.asMicros
        val explicitValidTo = (docMap.remove("_valid_to") as? Instant)?.asMicros

        if (explicitValidTo != null && explicitValidFrom == null)
            throw Incorrect("'_valid_to' requires '_valid_from'", "xtdb.kafka-source/docs-valid-to-without-from")

        openTxTable.logPut(
            ByteBuffer.wrap(id.asIid),
            explicitValidFrom ?: openTx.systemFrom,
            explicitValidTo ?: Long.MAX_VALUE,
        ) { openTxTable.docWriter.writeObject(docMap) }
    }
}

private fun parseTable(table: String): Pair<String, String> {
    val dot = table.indexOf('.')
    return if (dot < 0) DEFAULT_SCHEMA to table
    else table.substring(0, dot) to table.substring(dot + 1)
}
