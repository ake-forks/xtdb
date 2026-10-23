package xtdb.kafka.connectsrc

import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import org.apache.kafka.connect.sink.SinkRecord
import xtdb.api.error.Unsupported
import xtdb.api.tx.TxIndexer
import xtdb.kafka.connectsrc.proto.KafkaConnectSourceToken
import java.util.ServiceLoader
import com.google.protobuf.Any as ProtoAny

interface RecordIndexer : AutoCloseable {

    /**
     * Called at the start of each leader term, before any [indexRecords] in that term.
     *
     * The indexer outlives the term, so state belonging to the previous term — a partly-buffered batch, say —
     * is discarded here, and anything the indexer keeps across terms is restored from [userToken].
     *
     * @param userToken the `user_token` of the last indexed transaction's [KafkaConnectSourceToken],
     *   or null if no transaction has been indexed or that token carried none.
     *   An indexer that keeps state across terms writes it there when it submits a transaction.
     */
    fun beginTerm(userToken: ByteArray?) = Unit

    suspend fun indexRecords(records: List<SinkRecord>, txIndexer: TxIndexer)

    override fun close() = Unit

    interface Factory {
        fun open(): RecordIndexer

        companion object {
            private val registrations = ServiceLoader.load(Registration::class.java).toList()
            private val registrationsByTag = registrations.associateBy { it.protoTag }
            private val registrationsByClass = registrations.associateBy { it.factoryClass }

            val serializersModule = SerializersModule {
                for (reg in registrations)
                    include(reg.serializersModule)

                polymorphic(Factory::class) {
                    for (reg in registrations)
                        reg.registerSerde(this)
                }
            }

            fun fromProto(any: ProtoAny): Factory {
                val reg = registrationsByTag[any.typeUrl]
                    ?: error("unknown record indexer: ${any.typeUrl}")
                return reg.fromProto(any)
            }

            // A factory is persistable iff a Registration claims its class. A programmatically-supplied
            // indexer with no Registration is a legal Factory, but can't travel as a serialised secondary.
            @Suppress("UNCHECKED_CAST")
            fun toProto(factory: Factory): ProtoAny {
                val reg = registrationsByClass[factory.javaClass] as Registration<Factory>?
                    ?: throw Unsupported(
                        "record indexer ${factory.javaClass.name} can't be persisted as a secondary database — " +
                            "it has no Registration",
                        "xtdb.kafka-connect-source/indexer-not-serializable",
                        mapOf("indexer" to factory.javaClass.name),
                    )
                return reg.toProto(factory)
            }
        }
    }

    interface Registration<F : Factory> {
        val protoTag: String
        val factoryClass: Class<F>
        fun toProto(factory: F): ProtoAny
        fun fromProto(msg: ProtoAny): F
        fun registerSerde(builder: PolymorphicModuleBuilder<Factory>)
        val serializersModule: SerializersModule get() = SerializersModule {}
    }
}
