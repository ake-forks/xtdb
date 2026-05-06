package xtdb.kafkasource

import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import org.apache.kafka.clients.consumer.ConsumerRecord
import xtdb.indexer.TxIndexer
import java.util.ServiceLoader
import com.google.protobuf.Any as ProtoAny

/**
 * Maps a batch of Kafka records into XT transactions.
 *
 * Type alignment between [K] / [V] and the consumer's deserializers is by trust:
 * the framework opens a [org.apache.kafka.clients.consumer.KafkaConsumer] with the
 * deserializers configured on the [KafkaTopicSource] and feeds records straight in.
 * A mismatch surfaces as a [ClassCastException] at this call site, not at config time.
 */
interface RecordIndexer<K, V> : AutoCloseable {

    suspend fun indexRecords(records: List<ConsumerRecord<K, V>>, txIndexer: TxIndexer)

    override fun close() = Unit

    interface Factory<K, V> {
        fun toProto(): ProtoAny

        fun open(dbName: String): RecordIndexer<K, V>

        companion object {
            private val registrations = ServiceLoader.load(Registration::class.java).toList()
            private val registrationsByTag = registrations.associateBy { it.protoTag }

            val serializersModule = SerializersModule {
                for (reg in registrations)
                    include(reg.serializersModule)

                polymorphic(Factory::class) {
                    for (reg in registrations)
                        reg.registerSerde(this)
                }
            }

            fun fromProto(any: ProtoAny): Factory<*, *> {
                val reg = registrationsByTag[any.typeUrl]
                    ?: error("unknown record indexer: ${any.typeUrl}")
                return reg.fromProto(any)
            }
        }
    }

    interface Registration {
        val protoTag: String
        fun fromProto(msg: ProtoAny): Factory<*, *>
        fun registerSerde(builder: PolymorphicModuleBuilder<Factory<*, *>>)
        val serializersModule: SerializersModule get() = SerializersModule {}
    }
}
