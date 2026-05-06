package xtdb.kafkasource

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.isActive
import kotlinx.coroutines.runInterruptible
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.subclass
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.InterruptException
import org.apache.kafka.common.errors.RecordDeserializationException
import org.apache.kafka.common.errors.WakeupException
import xtdb.api.Remote
import xtdb.api.RemoteAlias
import xtdb.api.log.KafkaCluster
import xtdb.database.ExternalSource
import xtdb.database.ExternalSourceToken
import xtdb.database.proto.DatabaseConfig
import xtdb.error.Fault
import xtdb.error.Incorrect
import xtdb.indexer.TxIndexer
import xtdb.indexer.TxIndexer.TxResult
import xtdb.kafkasource.proto.KafkaTopicSourceConfig
import xtdb.kafkasource.proto.KafkaTopicSourceToken
import xtdb.kafkasource.proto.kafkaTopicSourceConfig
import xtdb.kafkasource.proto.kafkaTopicSourceToken
import xtdb.util.error
import xtdb.util.info
import xtdb.util.logger
import xtdb.util.warn
import java.time.Duration
import com.google.protobuf.Any as ProtoAny

private val LOG = KafkaTopicSource::class.logger

private const val PROTO_TAG_PREFIX = "proto.xtdb.com"

private val POLL_DURATION: Duration = Duration.ofSeconds(1)

// Tier-3 invariants the user can't override.
// `enable.auto.commit=false` — the resume token is our offset, Kafka must not race us.
// `auto.offset.reset=earliest` — a missing offset is an epoch concern, not a per-source toggle.
private val HARDCODED_CONSUMER_CONFIG: Map<String, String> = mapOf(
    "enable.auto.commit" to "false",
    "auto.offset.reset" to "earliest",
)

class KafkaTopicSource(
    private val dbName: String,
    private val cluster: KafkaCluster,
    private val topic: String,
    private val consumerConfig: Map<String, String>,
    private val indexer: RecordIndexer<Any?, Any?>,
) : ExternalSource {

    @Serializable
    @SerialName("!KafkaTopic")
    data class Factory(
        val remote: RemoteAlias,
        val topic: String,
        val consumerConfig: Map<String, String> = emptyMap(),
        val indexer: RecordIndexer.Factory<*, *>,
    ) : ExternalSource.Factory {

        override fun open(
            dbName: String,
            remotes: Map<RemoteAlias, Remote>,
        ): ExternalSource {
            val raw = remotes[remote]
                ?: throw Incorrect(
                    "no remote configured with alias '$remote' — add a '!Kafka' entry under 'remotes:' in node config",
                    errorCode = "xtdb.kafka-source/missing-remote",
                    data = mapOf("alias" to remote),
                )

            val actualType = raw::class.simpleName ?: raw::class.qualifiedName ?: "unknown"

            val cluster = raw as? KafkaCluster
                ?: throw Incorrect(
                    "remote '$remote' is a $actualType, expected a !Kafka remote",
                    errorCode = "xtdb.kafka-source/wrong-remote-type",
                    data = mapOf("alias" to remote, "actualType" to actualType),
                )

            // By-trust K/V alignment: the Factory is star-projected (kotlinx-serialization
            // can't carry K/V through the polymorphic decode), so the concrete deserializers
            // configured in `consumerConfig` are taken on faith to match the indexer's
            // declared K/V. Mismatches surface as ClassCastException at the indexer call site.
            @Suppress("UNCHECKED_CAST")
            val openedIndexer = (indexer as RecordIndexer.Factory<Any?, Any?>).open(dbName)

            return KafkaTopicSource(dbName, cluster, topic, consumerConfig, openedIndexer)
        }

        override fun writeTo(dbConfig: DatabaseConfig.Builder) {
            dbConfig.externalSource = ProtoAny.pack(kafkaTopicSourceConfig {
                remote = this@Factory.remote
                topic = this@Factory.topic
                consumerConfig.putAll(this@Factory.consumerConfig)
                indexer = this@Factory.indexer.toProto()
            }, PROTO_TAG_PREFIX)
        }

        class Registration : ExternalSource.Registration {
            override val protoTag: String
                get() = "$PROTO_TAG_PREFIX/xtdb.kafkasource.proto.KafkaTopicSourceConfig"

            override fun fromProto(msg: ProtoAny): ExternalSource.Factory {
                val config = msg.unpack(KafkaTopicSourceConfig::class.java)
                return Factory(
                    remote = config.remote,
                    topic = config.topic,
                    consumerConfig = config.consumerConfigMap,
                    indexer = RecordIndexer.Factory.fromProto(config.indexer),
                )
            }

            override fun registerSerde(builder: PolymorphicModuleBuilder<ExternalSource.Factory>) {
                builder.subclass(Factory::class)
            }

            // pulled in so YAML decode of !KafkaTopic can dispatch the nested polymorphic indexer
            override val serializersModule: SerializersModule = RecordIndexer.Factory.serializersModule
        }
    }

    override suspend fun onPartitionAssigned(
        partition: Int,
        afterToken: ExternalSourceToken?,
        txIndexer: TxIndexer,
    ) {
        LOG.info("[$dbName] Partition $partition assigned (topic=$topic)")

        val mergedConfig: Map<String, Any> = cluster.kafkaConfigMap + consumerConfig + HARDCODED_CONSUMER_CONFIG

        val consumer = KafkaConsumer<Any?, Any?>(mergedConfig)

        try {
            val tp = TopicPartition(topic, partition)
            consumer.assign(listOf(tp))

            val resumeFrom = afterToken?.let { KafkaTopicSourceToken.parseFrom(it).offset + 1 }
            if (resumeFrom != null) {
                LOG.info("[$dbName] Resuming from offset $resumeFrom on $topic-$partition")
                consumer.seek(tp, resumeFrom)
            } else {
                LOG.info("[$dbName] Starting from earliest on $topic-$partition")
            }

            while (currentCoroutineContext().isActive) {
                val records = try {
                    runInterruptible(Dispatchers.IO) { consumer.poll(POLL_DURATION) }
                } catch (_: WakeupException) {
                    break
                } catch (_: InterruptException) {
                    break
                } catch (e: RecordDeserializationException) {
                    handleDeserializationFailure(consumer, e, txIndexer)
                    continue
                }

                if (records.isEmpty) continue

                indexer.indexRecords(records.records(tp), txIndexer)
            }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            LOG.error(e, "[$dbName] External source failed")
            throw e
        } finally {
            runCatching { consumer.close() }
                .onFailure { LOG.error(it, "[$dbName] Consumer close failed") }
        }
    }

    private suspend fun handleDeserializationFailure(
        consumer: KafkaConsumer<Any?, Any?>,
        e: RecordDeserializationException,
        txIndexer: TxIndexer,
    ) {
        val tp = e.topicPartition()
        val offset = e.offset()
        val token = kafkaTopicSourceToken { this.offset = offset }.toByteArray()
        val coords = mapOf("topic" to tp.topic(), "partition" to tp.partition(), "offset" to offset)

        LOG.warn(e, "[$dbName] Deserialization failure at $tp offset=$offset — aborting tx, seeking past")

        val wrapped = Fault(
            "Kafka record deserialization failed at ${tp.topic()}-${tp.partition()} offset $offset",
            "xtdb.kafka-source/deserialization-failed",
            coords,
            cause = e,
        )

        txIndexer.indexTx(token) { TxResult.Aborted(wrapped, userMetadata = coords) }

        // without this seek the next poll re-trips the same exception
        consumer.seek(tp, offset + 1)
    }

    override fun close() {
        LOG.info("[$dbName] Closing external source")
        runCatching { indexer.close() }
            .onFailure { LOG.error(it, "[$dbName] Indexer close failed") }
    }
}
