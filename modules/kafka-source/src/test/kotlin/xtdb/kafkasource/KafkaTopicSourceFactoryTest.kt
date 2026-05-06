package xtdb.kafkasource

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import xtdb.database.Database

class KafkaTopicSourceFactoryTest {

    private fun protoRoundTrip(factory: KafkaTopicSource.Factory): KafkaTopicSource.Factory {
        val dbConfig = Database.Config().externalSource(factory)
        val restored = Database.Config.fromProto(dbConfig.serializedConfig)
        return restored.externalSource as KafkaTopicSource.Factory
    }

    @Test
    fun `proto round-trips factory`() {
        val original = KafkaTopicSource.Factory(
            remote = "my-kafka",
            topic = "orders",
            consumerConfig = mapOf(
                "key.deserializer" to "org.apache.kafka.common.serialization.StringDeserializer",
                "value.deserializer" to "io.confluent.kafka.serializers.KafkaJsonDeserializer",
            ),
            indexer = DocsIndexer.Factory(table = "orders"),
        )

        val restored = protoRoundTrip(original)

        assertEquals("my-kafka", restored.remote)
        assertEquals("orders", restored.topic)
        assertEquals(
            "org.apache.kafka.common.serialization.StringDeserializer",
            restored.consumerConfig["key.deserializer"],
        )
        val docs = restored.indexer as DocsIndexer.Factory
        assertEquals("orders", docs.table)
    }

    @Test
    fun `YAML round-trips !KafkaTopic with !Docs indexer`() {
        val yaml = """
            externalSource: !KafkaTopic
              remote: my-kafka
              topic: orders
              consumerConfig:
                key.deserializer: org.apache.kafka.common.serialization.StringDeserializer
                value.deserializer: io.confluent.kafka.serializers.KafkaJsonDeserializer
              indexer: !Docs
                table: orders
        """.trimIndent()

        val config = Database.Config.fromYaml(yaml)
        val factory = config.externalSource as KafkaTopicSource.Factory

        assertEquals("my-kafka", factory.remote)
        assertEquals("orders", factory.topic)
        assertEquals(
            "io.confluent.kafka.serializers.KafkaJsonDeserializer",
            factory.consumerConfig["value.deserializer"],
        )
        val docs = factory.indexer as DocsIndexer.Factory
        assertEquals("orders", docs.table)
    }

    @Test
    fun `consumerConfig defaults to empty map`() {
        val yaml = """
            externalSource: !KafkaTopic
              remote: k
              topic: t
              indexer: !Docs
                table: orders
        """.trimIndent()

        val factory = Database.Config.fromYaml(yaml).externalSource as KafkaTopicSource.Factory

        assertTrue(factory.consumerConfig.isEmpty())
    }

    @Test
    fun `qualified table name parses to schema + table`() {
        // !Docs accepts 'public.orders' or 'orders'; the split happens in Factory.open(),
        // not at config time — config just stores the raw string.
        val yaml = """
            externalSource: !KafkaTopic
              remote: k
              topic: t
              indexer: !Docs
                table: analytics.events
        """.trimIndent()

        val factory = Database.Config.fromYaml(yaml).externalSource as KafkaTopicSource.Factory
        val docs = factory.indexer as DocsIndexer.Factory

        assertEquals("analytics.events", docs.table)
    }
}
