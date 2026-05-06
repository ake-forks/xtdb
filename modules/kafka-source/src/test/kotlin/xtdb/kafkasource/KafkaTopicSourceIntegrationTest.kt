package xtdb.kafkasource

import kotlinx.coroutines.runInterruptible
import kotlinx.coroutines.test.runTest
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.containers.Network
import org.testcontainers.kafka.ConfluentKafkaContainer
import org.testcontainers.lifecycle.Startables
import xtdb.api.Xtdb
import xtdb.api.log.KafkaCluster
import java.util.UUID
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

@Tag("integration")
class KafkaTopicSourceIntegrationTest {

    companion object {
        private val network: Network = Network.newNetwork()

        private val kafka = ConfluentKafkaContainer("confluentinc/cp-kafka:7.8.0")
            .withNetwork(network)
            .withNetworkAliases("kafka")

        @JvmStatic
        @BeforeAll
        fun beforeAll() {
            Startables.deepStart(kafka).join()
        }

        @JvmStatic
        @AfterAll
        fun afterAll() {
            kafka.stop()
            network.close()
        }
    }

    private fun createTopic(topic: String) {
        AdminClient.create(mapOf("bootstrap.servers" to kafka.bootstrapServers)).use { admin ->
            admin.createTopics(listOf(NewTopic(topic, 1, 1.toShort()))).all().get()
        }
    }

    private fun openNode(sourceTopic: String): Xtdb = Xtdb.openNode {
        server { port = 0 }; flightSql = null
        logCluster("kafka", KafkaCluster.ClusterFactory(kafka.bootstrapServers))
        log(KafkaCluster.LogFactory("kafka", sourceTopic))
    }

    private fun attachKafkaTopicSource(
        node: Xtdb,
        sourceTopic: String,
        keyDeserializer: String,
        valueDeserializer: String,
        dbName: String = "events",
        table: String = "events",
    ) {
        node.getConnection().use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(
                    """
                    ATTACH DATABASE $dbName WITH ${'$'}${'$'}
                        log: !Kafka
                          cluster: kafka
                          topic: test-replica-${UUID.randomUUID()}
                        externalSource: !KafkaTopic
                          remote: kafka
                          topic: $sourceTopic
                          consumerConfig:
                            key.deserializer: $keyDeserializer
                            value.deserializer: $valueDeserializer
                          indexer: !Docs
                            table: $table
                    ${'$'}${'$'}
                    """.trimIndent()
                )
            }
        }
    }

    private fun produceJson(topic: String, key: ByteArray?, valueJson: String?, keyClass: Class<*> = StringSerializer::class.java) {
        val producerProps = mapOf(
            "bootstrap.servers" to kafka.bootstrapServers,
            "key.serializer" to keyClass.name,
            "value.serializer" to ByteArraySerializer::class.java.name,
        )
        KafkaProducer<Any?, ByteArray?>(producerProps).use { producer ->
            val key0: Any? = key?.let { if (keyClass == StringSerializer::class.java) String(it) else it }
            val rec = ProducerRecord<Any?, ByteArray?>(topic, key0, valueJson?.toByteArray(Charsets.UTF_8))
            producer.send(rec).get()
        }
    }

    private fun xtQueryDb(node: Xtdb, dbName: String, sql: String): List<Map<String, Any?>> =
        node.createConnectionBuilder().database(dbName).build().use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeQuery(sql).use { rs ->
                    val md = rs.metaData
                    val cols = (1..md.columnCount).map { md.getColumnName(it) }
                    buildList {
                        while (rs.next()) add(cols.associateWith { rs.getObject(it) })
                    }
                }
            }
        }

    private suspend fun awaitCondition(description: String, timeout: Duration = 30.seconds, check: () -> Boolean) {
        val deadline = System.currentTimeMillis() + timeout.inWholeMilliseconds
        while (System.currentTimeMillis() < deadline) {
            if (runCatching(check).getOrDefault(false)) return
            runInterruptible { Thread.sleep(200) }
        }
        throw AssertionError("Timed out waiting for: $description")
    }

    @Test
    fun `String key + JSON value snapshot + streaming`() = runTest(timeout = 120.seconds) {
        val sourceTopic = "events-${UUID.randomUUID()}"
        createTopic(sourceTopic)

        // Pre-existing records (consumer will read from offset 0).
        produceJson(sourceTopic, "k1".toByteArray(), """{"name":"Alice","age":30}""")
        produceJson(sourceTopic, "k2".toByteArray(), """{"_id":"explicit-id","name":"Bob"}""")

        openNode("xt-log-${UUID.randomUUID()}").use { node ->
            attachKafkaTopicSource(
                node,
                sourceTopic = sourceTopic,
                keyDeserializer = "org.apache.kafka.common.serialization.StringDeserializer",
                valueDeserializer = MapJsonDeserializer::class.java.name,
            )

            awaitCondition("both records appear") {
                xtQueryDb(node, "events", "SELECT _id FROM public.events").size == 2
            }

            // k1 has no _id in doc → key 'k1' becomes _id.
            val k1 = xtQueryDb(node, "events", "SELECT _id, name, age FROM public.events WHERE _id = 'k1'")
            assertEquals(1, k1.size)
            assertEquals("Alice", k1[0]["name"])

            // k2 has explicit _id in doc → that wins over the record key.
            val k2 = xtQueryDb(node, "events", "SELECT _id, name FROM public.events WHERE _id = 'explicit-id'")
            assertEquals(1, k2.size)
            assertEquals("Bob", k2[0]["name"])

            // Streaming — write a record after the node is up and consuming.
            produceJson(sourceTopic, "k3".toByteArray(), """{"name":"Charlie"}""")

            awaitCondition("streamed record appears") {
                xtQueryDb(node, "events", "SELECT _id FROM public.events WHERE _id = 'k3'").isNotEmpty()
            }
        }
    }

    @Test
    fun `null value tombstone deletes the row`() = runTest(timeout = 120.seconds) {
        val sourceTopic = "events-${UUID.randomUUID()}"
        createTopic(sourceTopic)

        produceJson(sourceTopic, "k1".toByteArray(), """{"name":"Alice"}""")

        openNode("xt-log-${UUID.randomUUID()}").use { node ->
            attachKafkaTopicSource(
                node,
                sourceTopic = sourceTopic,
                keyDeserializer = "org.apache.kafka.common.serialization.StringDeserializer",
                valueDeserializer = MapJsonDeserializer::class.java.name,
            )

            awaitCondition("Alice ingested") {
                xtQueryDb(node, "events", "SELECT _id FROM public.events WHERE _id = 'k1'").isNotEmpty()
            }

            // Tombstone — produce a record with null value for the same key.
            produceJson(sourceTopic, "k1".toByteArray(), valueJson = null)

            awaitCondition("Alice deleted") {
                xtQueryDb(node, "events", "SELECT _id FROM public.events WHERE _id = 'k1'").isEmpty()
            }
        }
    }

    @Test
    fun `ByteArray key + JSON value`() = runTest(timeout = 120.seconds) {
        val sourceTopic = "events-${UUID.randomUUID()}"
        createTopic(sourceTopic)

        // Produce with a string key but consume it as bytes — the indexer's _id
        // resolution falls back to the record key as bytes, which `asIid` handles.
        produceJson(sourceTopic, "key-bytes-1".toByteArray(), """{"_id":"in-doc-1","value":42}""")
        produceJson(sourceTopic, "key-bytes-2".toByteArray(), """{"_id":"in-doc-2","value":99}""")

        openNode("xt-log-${UUID.randomUUID()}").use { node ->
            attachKafkaTopicSource(
                node,
                sourceTopic = sourceTopic,
                keyDeserializer = "org.apache.kafka.common.serialization.ByteArrayDeserializer",
                valueDeserializer = MapJsonDeserializer::class.java.name,
            )

            awaitCondition("both records appear") {
                xtQueryDb(node, "events", "SELECT _id FROM public.events").size == 2
            }

            val rows = xtQueryDb(node, "events", "SELECT _id, value FROM public.events ORDER BY _id")
            assertEquals("in-doc-1", rows[0]["_id"])
            assertEquals(42L, rows[0]["value"])
        }
    }

    @Test
    fun `resume from token after restart`() = runTest(timeout = 180.seconds) {
        val sourceTopic = "events-${UUID.randomUUID()}"
        val xtLog = "xt-log-${UUID.randomUUID()}"
        createTopic(sourceTopic)

        produceJson(sourceTopic, "a".toByteArray(), """{"name":"Alice"}""")

        // Phase 1: snapshot Alice, then stream Bob.
        openNode(xtLog).use { node ->
            attachKafkaTopicSource(
                node,
                sourceTopic = sourceTopic,
                keyDeserializer = "org.apache.kafka.common.serialization.StringDeserializer",
                valueDeserializer = MapJsonDeserializer::class.java.name,
            )

            awaitCondition("Alice appears") {
                xtQueryDb(node, "events", "SELECT _id FROM public.events WHERE _id = 'a'").isNotEmpty()
            }

            produceJson(sourceTopic, "b".toByteArray(), """{"name":"Bob"}""")
            awaitCondition("Bob appears") {
                xtQueryDb(node, "events", "SELECT _id FROM public.events WHERE _id = 'b'").isNotEmpty()
            }
        }

        // Produce Charlie while the node is down — it must be picked up after restart.
        produceJson(sourceTopic, "c".toByteArray(), """{"name":"Charlie"}""")

        // Phase 2: restart against the same source/replica logs. The node replays
        // the ATTACH DATABASE from the source log, recovers the resume token,
        // seeks past the consumed offsets, and picks up Charlie.
        openNode(xtLog).use { node ->
            awaitCondition("Charlie appears after restart", timeout = 60.seconds) {
                xtQueryDb(node, "events", "SELECT _id FROM public.events WHERE _id = 'c'").isNotEmpty()
            }

            val rows = xtQueryDb(node, "events", "SELECT _id, name FROM public.events ORDER BY _id")
            assertEquals(3, rows.size, "All three rows present — no duplication, no loss")
            assertEquals("Alice", rows[0]["name"])
            assertEquals("Bob", rows[1]["name"])
            assertEquals("Charlie", rows[2]["name"])
        }
    }

    @Test
    fun `explicit _valid_from and _valid_to are honoured`() = runTest(timeout = 120.seconds) {
        val sourceTopic = "events-${UUID.randomUUID()}"
        createTopic(sourceTopic)

        // _valid_from in the past, _valid_to in the future.
        produceJson(
            sourceTopic, "vt".toByteArray(),
            """{"name":"Bounded","_valid_from":"2020-01-01T00:00:00Z","_valid_to":"2030-01-01T00:00:00Z"}""",
        )

        openNode("xt-log-${UUID.randomUUID()}").use { node ->
            attachKafkaTopicSource(
                node,
                sourceTopic = sourceTopic,
                keyDeserializer = "org.apache.kafka.common.serialization.StringDeserializer",
                valueDeserializer = MapJsonDeserializer::class.java.name,
            )

            awaitCondition("record ingested with explicit bounds") {
                xtQueryDb(node, "events", "SELECT _id FROM public.events WHERE _id = 'vt'").isNotEmpty()
            }

            // Visible inside the bounds.
            val inside = xtQueryDb(
                node, "events",
                "SELECT name FROM public.events FOR VALID_TIME AS OF DATE '2025-06-01' WHERE _id = 'vt'",
            )
            assertEquals(1, inside.size)
            assertEquals("Bounded", inside[0]["name"])

            // Not visible before _valid_from.
            val before = xtQueryDb(
                node, "events",
                "SELECT _id FROM public.events FOR VALID_TIME AS OF DATE '2019-01-01' WHERE _id = 'vt'",
            )
            assertTrue(before.isEmpty(), "row should not be visible before _valid_from, got: $before")
        }
    }

    @Test
    fun `_valid_to without _valid_from produces aborted tx`() = runTest(timeout = 120.seconds) {
        val sourceTopic = "events-${UUID.randomUUID()}"
        createTopic(sourceTopic)

        produceJson(
            sourceTopic, "bad".toByteArray(),
            """{"name":"BadBounds","_valid_to":"2030-01-01T00:00:00Z"}""",
        )
        // Follow with a good record so we know the consumer continued past the bad one.
        produceJson(sourceTopic, "ok".toByteArray(), """{"name":"OK"}""")

        openNode("xt-log-${UUID.randomUUID()}").use { node ->
            attachKafkaTopicSource(
                node,
                sourceTopic = sourceTopic,
                keyDeserializer = "org.apache.kafka.common.serialization.StringDeserializer",
                valueDeserializer = MapJsonDeserializer::class.java.name,
            )

            awaitCondition("good record committed past bad one") {
                xtQueryDb(node, "events", "SELECT _id FROM public.events WHERE _id = 'ok'").isNotEmpty()
            }

            assertTrue(
                xtQueryDb(node, "events", "SELECT _id FROM public.events WHERE _id = 'bad'").isEmpty(),
                "bad record should have been aborted, not visible in the table",
            )

            val aborted = xtQueryDb(node, "events", "SELECT committed, error FROM xt.txs WHERE committed = false")
            assertTrue(aborted.isNotEmpty(), "expected an aborted tx for the bad-bounds record, got none")
        }
    }

    @Test
    fun `non-map value produces aborted tx`() = runTest(timeout = 120.seconds) {
        val sourceTopic = "events-${UUID.randomUUID()}"
        createTopic(sourceTopic)

        // Top-level JSON array, not an object — MapJsonDeserializer rejects this,
        // so the failure surfaces at the deserializer (RecordDeserializationException).
        // To exercise the !Docs "non-map value" path we'd need a deserializer that produces
        // a non-Map without throwing. Use Json that's a top-level number string instead:
        // the MapJsonDeserializer will throw, the framework abort-and-seek-past handles it.
        // For a bona-fide indexer-side non-map, see the unit-level coverage.
        // (Both paths land in xt.txs as aborted txs with topic/partition/offset coords.)
        produceJson(sourceTopic, "n".toByteArray(), "42")
        produceJson(sourceTopic, "ok".toByteArray(), """{"name":"OK"}""")

        openNode("xt-log-${UUID.randomUUID()}").use { node ->
            attachKafkaTopicSource(
                node,
                sourceTopic = sourceTopic,
                keyDeserializer = "org.apache.kafka.common.serialization.StringDeserializer",
                valueDeserializer = MapJsonDeserializer::class.java.name,
            )

            awaitCondition("good record committed past bad one") {
                xtQueryDb(node, "events", "SELECT _id FROM public.events WHERE _id = 'ok'").isNotEmpty()
            }

            val aborted = xtQueryDb(node, "events", "SELECT committed FROM xt.txs WHERE committed = false")
            assertTrue(aborted.isNotEmpty(), "expected an aborted tx for the non-map value")
        }
    }

    @Test
    fun `tombstone with no key produces aborted tx`() = runTest(timeout = 120.seconds) {
        val sourceTopic = "events-${UUID.randomUUID()}"
        createTopic(sourceTopic)

        // Null key + null value → !Docs can't resolve _id → aborted tx.
        produceJson(sourceTopic, key = null, valueJson = null)
        produceJson(sourceTopic, "ok".toByteArray(), """{"name":"OK"}""")

        openNode("xt-log-${UUID.randomUUID()}").use { node ->
            attachKafkaTopicSource(
                node,
                sourceTopic = sourceTopic,
                keyDeserializer = "org.apache.kafka.common.serialization.StringDeserializer",
                valueDeserializer = MapJsonDeserializer::class.java.name,
            )

            awaitCondition("good record committed past bad tombstone") {
                xtQueryDb(node, "events", "SELECT _id FROM public.events WHERE _id = 'ok'").isNotEmpty()
            }

            val aborted = xtQueryDb(node, "events", "SELECT committed FROM xt.txs WHERE committed = false")
            assertTrue(aborted.isNotEmpty(), "expected an aborted tx for the keyless tombstone")
        }
    }

    @Test
    fun `qualified schema dot table routes to the named schema`() = runTest(timeout = 120.seconds) {
        val sourceTopic = "events-${UUID.randomUUID()}"
        createTopic(sourceTopic)

        produceJson(sourceTopic, "x".toByteArray(), """{"name":"InAnalytics"}""")

        openNode("xt-log-${UUID.randomUUID()}").use { node ->
            attachKafkaTopicSource(
                node,
                sourceTopic = sourceTopic,
                keyDeserializer = "org.apache.kafka.common.serialization.StringDeserializer",
                valueDeserializer = MapJsonDeserializer::class.java.name,
                table = "analytics.events",
            )

            awaitCondition("record visible in analytics.events") {
                xtQueryDb(node, "events", "SELECT _id FROM analytics.events WHERE _id = 'x'").isNotEmpty()
            }

            assertTrue(
                xtQueryDb(node, "events", "SELECT _id FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'events'").isEmpty(),
                "no public.events table should have been created — the indexer wrote to analytics.events",
            )
        }
    }

    @Test
    fun `deserialization failure produces aborted xt tx`() = runTest(timeout = 120.seconds) {
        val sourceTopic = "events-${UUID.randomUUID()}"
        createTopic(sourceTopic)

        // Bad bytes that MapJsonDeserializer can't parse as JSON.
        produceJson(sourceTopic, "bad".toByteArray(), valueJson = "this-is-not-json")
        // Then a good record so we can verify the consumer seeks past the bad one.
        produceJson(sourceTopic, "ok".toByteArray(), """{"name":"OK"}""")

        openNode("xt-log-${UUID.randomUUID()}").use { node ->
            attachKafkaTopicSource(
                node,
                sourceTopic = sourceTopic,
                keyDeserializer = "org.apache.kafka.common.serialization.StringDeserializer",
                valueDeserializer = MapJsonDeserializer::class.java.name,
            )

            // Bad record causes an aborted tx; good record commits — at least one of each in xt.txs.
            awaitCondition("good record committed") {
                xtQueryDb(node, "events", "SELECT _id FROM public.events WHERE _id = 'ok'").isNotEmpty()
            }

            val txs = xtQueryDb(node, "events", "SELECT committed, error, user_metadata FROM xt.txs")
            val aborted = txs.filter { it["committed"] == false }
            assertTrue(aborted.isNotEmpty(), "expected an aborted tx for the deser failure, got: $txs")
            assertNotNull(aborted[0]["error"], "aborted tx should carry the error")

            // Coords should ride on user_metadata so operators can triage from the tx log
            // without unpacking the exception payload.
            val metadata = aborted[0]["user_metadata"]
            assertNotNull(metadata, "aborted tx should carry user_metadata with coords")
            val mdString = metadata.toString()
            assertTrue(mdString.contains(sourceTopic), "user_metadata should carry topic, got: $mdString")
            assertTrue(mdString.contains("offset"), "user_metadata should carry offset, got: $mdString")
        }
    }
}
