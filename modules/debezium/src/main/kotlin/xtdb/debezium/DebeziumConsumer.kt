package xtdb.debezium

import kotlinx.coroutines.*
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.InterruptException
import xtdb.api.log.Log
import xtdb.api.log.*
import xtdb.indexer.LogProcessor
import kotlin.coroutines.CoroutineContext
import java.time.Duration
import kotlin.time.Duration.Companion.seconds
import org.apache.arrow.memory.BufferAllocator
import org.apache.kafka.clients.consumer.ConsumerRecord
import xtdb.arrow.Relation
import xtdb.indexer.Indexer
import xtdb.tx.writeTo
import xtdb.util.MsgIdUtil
import xtdb.util.closeAll
import java.time.Instant

interface DebeziumConsumer : AutoCloseable {
    fun tailAll(afterMsgId: MessageId, processor: LogProcessor.ResolvedTxHandler): Log.Subscription

    sealed interface Factory {
        fun createConsumer(): DebeziumConsumer
    }

    class KafkaConsumerFactory(
        private val kafkaConfig: Map<String, String>,
        private val topic: String,
        private var pollDuration: Duration = Duration.ofSeconds(1),
        private val indexer: Indexer.ForDatabase,
        val allocator: BufferAllocator,
        coroutineContext: CoroutineContext = Dispatchers.Default
    ) : Factory {
        val scope = CoroutineScope(coroutineContext + Job())
        val epoch: Int get() = 0

        override fun createConsumer(): DebeziumConsumer = object : DebeziumConsumer {

            private fun resolveTx(record: ConsumerRecord<Unit, ByteArray>): ReplicaMessage.ResolvedTx {
                // TODO: Support SKIP_TXs?
                val recordTimestamp = Instant.ofEpochMilli(record.timestamp())
                val txOps = listOf(CdcEvent.fromJson(record.value()).toTxOp(allocator))
                try {
                    // NOTE: Don't do this, instead consider creating a relation directly and sending to the LiveIndex
                    txOps.toRelation(allocator, null).use { rel ->
                        indexer.indexTx(
                            record.offset(),
                            recordTimestamp,
                            rel["tx-ops"].listElements,
                            recordTimestamp, null, null, null
                        )
                    }
                } finally {
                    txOps.closeAll()
                }
            }

            override fun tailAll(afterMsgId: MessageId, processor: LogProcessor.ResolvedTxHandler): Log.Subscription {
                val afterOffset = MsgIdUtil.afterMsgIdToOffset(epoch, afterMsgId)

                val pollingJob = scope.launch(Dispatchers.IO) {
                    KafkaConsumer(
                        kafkaConfig + mapOf(
                            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false",
                        ),
                        UnitDeserializer,
                        ByteArrayDeserializer()
                    ).use { c ->
                        TopicPartition(topic, 0).also { tp ->
                            c.assign(listOf(tp))
                            c.seek(tp, afterOffset + 1)
                        }
                        while (isActive) {
                            runInterruptible {
                                val records = try {
                                    c.poll(pollDuration).records(topic)
                                } catch (_: InterruptException) {
                                    throw InterruptedException()
                                }

                                for (record in records) {
                                    processor.handleResolvedTx(resolveTx(record))
                                }
                            }
                        }
                    }
                }

                return Log.Subscription {
                    runBlocking { withTimeout(5.seconds) { pollingJob.cancelAndJoin() } }
                }
            }

            override fun close() {
                runBlocking { withTimeout (5.seconds) { scope.coroutineContext.job.cancelAndJoin() } }
            }
        }
    }
}

