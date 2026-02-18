package xtdb.debezium

import kotlinx.serialization.json.*
import org.apache.arrow.memory.BufferAllocator
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import xtdb.api.Xtdb
import xtdb.arrow.Relation
import xtdb.arrow.Vector
import xtdb.tx.TxOp
import xtdb.tx.TxOpts
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean

internal fun tsNsToInstant(source: JsonObject?): Instant? {
    val tsNs = source?.get("ts_ns")?.jsonPrimitive?.longOrNull ?: return null
    return Instant.ofEpochSecond(tsNs / 1_000_000_000, tsNs % 1_000_000_000)
}

internal fun jsonObjectToMap(obj: JsonObject): Map<String, Any?> =
    obj.entries.associate { (k, v) -> k to v.toJvmValue() }

internal fun JsonElement.toJvmValue(): Any? = when (this) {
    is JsonNull -> null
    is JsonPrimitive -> when {
        isString -> content
        content == "true" -> true
        content == "false" -> false
        content.contains('.') -> content.toDouble()
        else -> content.toLongOrNull() ?: content.toDouble()
    }
    is JsonArray -> map { it.toJvmValue() }
    is JsonObject -> jsonObjectToMap(this)
}

private val log = LoggerFactory.getLogger(DebeziumConsumer::class.java)

class DebeziumConsumer(
    private val node: Xtdb,
    private val dbName: String,
    private val allocator: BufferAllocator,
    private val bootstrapServers: String,
    private val topic: String,
    private val groupId: String,
    private val pollTimeout: Duration = Duration.ofSeconds(1),
) : AutoCloseable {

    private val running = AtomicBoolean(false)

    fun start() {
        running.set(true)

        val consumer = KafkaConsumer<String, String>(
            mapOf(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG to groupId,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
            )
        )

        consumer.use { c ->
            c.subscribe(listOf(topic))
            log.info("Subscribed to topic '{}', starting poll loop", topic)

            while (running.get()) {
                val records = c.poll(pollTimeout)
                if (!records.isEmpty) {
                    log.info("Polled {} CDC records from {}", records.count(), topic)
                }
                for (record in records) {
                    processRecord(record)
                }
                if (!records.isEmpty) {
                    c.commitSync()
                }
            }
        }
    }

    fun stop() {
        running.set(false)
    }

    private fun processRecord(record: ConsumerRecord<String, String>) {
        // Skip tombstones — null value records sent by Debezium for Kafka log compaction
        val value = record.value() ?: return

        try {
            val envelope = Json.parseToJsonElement(value).jsonObject
            val payload = envelope["payload"]?.jsonObject
                ?: throw IllegalArgumentException("Missing 'payload' in CDC message")

            val op = payload["op"]?.jsonPrimitive?.content
                ?: throw IllegalArgumentException("Missing 'op' in payload")

            val source = payload["source"]?.jsonObject
            val metadata = buildTxMetadata(source, record)

            val txOp = translateToTxOp(op, payload, source)

            txOp.use {
                val result = node.submitTx(dbName, listOf(it), TxOpts(userMetadata = metadata))
                log.debug("Submitted tx {} for CDC op '{}' at offset {}", result.txId, op, record.offset())
            }
        } catch (e: Exception) {
            log.error("Failed to process CDC record at offset {}: {}", record.offset(), e.message, e)
            submitDlq(e.message ?: "Unknown error", record)
        }
    }

    internal fun translateToTxOp(op: String, payload: JsonObject, source: JsonObject?): TxOp {
        val schemaName = source?.get("schema")?.jsonPrimitive?.content ?: "public"
        val tableName = source?.get("table")?.jsonPrimitive?.content
            ?: throw IllegalArgumentException("Missing 'source.table' in payload")

        return when (op) {
            "c", "r", "u" -> translatePutDocs(payload, source, schemaName, tableName)
            "d" -> translateDeleteDocs(payload, source, schemaName, tableName)
            else -> throw IllegalArgumentException("Unknown CDC op: '$op'")
        }
    }

    private fun translatePutDocs(
        payload: JsonObject,
        source: JsonObject?,
        schemaName: String,
        tableName: String,
    ): TxOp.PutDocs {
        val after = payload["after"]?.jsonObject
            ?: throw IllegalArgumentException("Missing 'after' for put op")

        val docMap = jsonObjectToMap(after).toMutableMap()

        if ("_id" !in docMap) {
            throw IllegalArgumentException("Missing '_id' in document")
        }

        val validFrom = (docMap.remove("_valid_from") as? String)?.let(Instant::parse)
            ?: tsNsToInstant(source)

        val validTo = (docMap.remove("_valid_to") as? String)?.let(Instant::parse)

        val rel = Relation.openFromRows(allocator, listOf(docMap))
        return TxOp.PutDocs(schemaName, tableName, validFrom, validTo, rel)
    }

    private fun translateDeleteDocs(
        payload: JsonObject,
        source: JsonObject?,
        schemaName: String,
        tableName: String,
    ): TxOp.DeleteDocs {
        val before = payload["before"]?.takeUnless { it is JsonNull }?.jsonObject
            ?: throw IllegalArgumentException("Missing 'before' for delete — check REPLICA IDENTITY on source table")

        val id = before["_id"]?.toJvmValue()
            ?: throw IllegalArgumentException("Missing '_id' in 'before' for delete")

        val validFrom = tsNsToInstant(source)

        val ids = Vector.fromList(allocator, "_id", listOf(id))
        return TxOp.DeleteDocs(schemaName, tableName, validFrom, null, ids)
    }

    private fun submitDlq(errorMessage: String, record: ConsumerRecord<String, String>) {
        node.submitTx(
            dbName, emptyList(),
            TxOpts(
                userMetadata = mapOf(
                    "debezium/error" to errorMessage,
                    "debezium/kafka-offset" to record.offset(),
                    "debezium/kafka-topic" to record.topic(),
                )
            )
        )
    }

    private fun buildTxMetadata(source: JsonObject?, record: ConsumerRecord<String, String>): Map<String, Any?> {
        val metadata = mutableMapOf<String, Any?>(
            "debezium/kafka-offset" to record.offset(),
            "debezium/kafka-topic" to record.topic(),
        )
        source?.get("lsn")?.jsonPrimitive?.longOrNull?.let {
            metadata["debezium/lsn"] = it
        }
        return metadata
    }

    override fun close() {
        stop()
    }
}
