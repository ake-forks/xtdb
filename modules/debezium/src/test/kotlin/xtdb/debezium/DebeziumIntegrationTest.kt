package xtdb.debezium

import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.*
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.kafka.ConfluentKafkaContainer
import org.testcontainers.lifecycle.Startables
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.sql.DriverManager
import java.time.Duration
import kotlin.time.Duration.Companion.seconds

@Tag("integration")
class DebeziumIntegrationTest {

    companion object {
        private val network: Network = Network.newNetwork()

        private val postgres = PostgreSQLContainer("postgres:17-alpine")
            .withNetwork(network)
            .withNetworkAliases("postgres")
            .withDatabaseName("testdb")
            .withUsername("testuser")
            .withPassword("testpass")
            .withCommand("postgres", "-c", "wal_level=logical")

        private val kafka = ConfluentKafkaContainer("confluentinc/cp-kafka:7.8.0")
            .withNetwork(network)
            .withNetworkAliases("kafka")
            .withListener("kafka:19092")

        private val debeziumConnect: GenericContainer<*> =
            GenericContainer("quay.io/debezium/connect:3.0")
                .withNetwork(network)
                .withExposedPorts(8083)
                .withEnv("BOOTSTRAP_SERVERS", "kafka:19092")
                .withEnv("GROUP_ID", "debezium-connect")
                .withEnv("CONFIG_STORAGE_TOPIC", "debezium_configs")
                .withEnv("OFFSET_STORAGE_TOPIC", "debezium_offsets")
                .withEnv("STATUS_STORAGE_TOPIC", "debezium_statuses")
                .waitingFor(Wait.forHttp("/connectors").forPort(8083).forStatusCode(200))
                .dependsOn(kafka)

        private val httpClient: HttpClient = HttpClient.newHttpClient()

        private fun connectUrl() =
            "http://${debeziumConnect.host}:${debeziumConnect.getMappedPort(8083)}"

        @JvmStatic
        @BeforeAll
        fun startContainers() {
            Startables.deepStart(postgres, kafka, debeziumConnect).join()
        }
    }

    private fun isConnectorRunning(): Boolean {
        try {
            val resp = httpClient.send(
                HttpRequest.newBuilder()
                    .uri(URI.create("${connectUrl()}/connectors/test-connector/status"))
                    .GET()
                    .build(),
                HttpResponse.BodyHandlers.ofString()
            )
            if (resp.statusCode() != 200) return false
            val status = Json.parseToJsonElement(resp.body()).jsonObject
            val connectorState = status["connector"]?.jsonObject?.get("state")?.jsonPrimitive?.content
            val taskState = status["tasks"]?.jsonArray?.firstOrNull()?.jsonObject?.get("state")?.jsonPrimitive?.content
            return connectorState == "RUNNING" && taskState == "RUNNING"
        } catch (_: Exception) {
            return false
        }
    }

    private fun registerConnector() {
        val connectorConfig = buildJsonObject {
            put("name", "test-connector")
            putJsonObject("config") {
                put("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
                put("tasks.max", "1")
                put("database.hostname", "postgres")
                put("database.port", "5432")
                put("database.user", "testuser")
                put("database.password", "testpass")
                put("database.dbname", "testdb")
                put("topic.prefix", "testdb")
                put("schema.include.list", "public")
                put("plugin.name", "pgoutput")
            }
        }

        val request = HttpRequest.newBuilder()
            .uri(URI.create("${connectUrl()}/connectors"))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(connectorConfig.toString()))
            .build()

        val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
        assertTrue(response.statusCode() in 200..201, "Failed to register connector: ${response.body()}")
    }

    private suspend fun registerConnectorAndAwait() {
        registerConnector()
        while (!isConnectorRunning()) delay(500)
    }

    private fun executeSql(vararg statements: String) {
        DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password).use { conn ->
            conn.createStatement().use { stmt ->
                for (sql in statements) stmt.execute(sql)
            }
        }
    }

    private suspend fun pollMessages(topic: String, expected: Int): List<JsonObject> {
        val props = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to kafka.bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG to "test-consumer",
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
        )

        return KafkaConsumer<String, String>(props).use { consumer ->
            consumer.subscribe(listOf(topic))

            val messages = mutableListOf<JsonObject>()

            while (messages.size < expected) {
                val records = consumer.poll(Duration.ofSeconds(1))
                for (record in records) {
                    // Debezium sends tombstone records (null value) after deletes for log compaction
                    val value = record.value() ?: continue
                    messages.add(Json.parseToJsonElement(value).jsonObject)
                }
                delay(100)
            }

            assertEquals(expected, messages.size, "Expected $expected CDC messages on $topic, got ${messages.size}")
            messages
        }
    }

    private fun JsonObject.payload(): JsonObject =
        this["payload"]?.jsonObject ?: fail("Expected 'payload' key in message")

    private fun assertCdcEvent(message: JsonObject, expectedOp: String, after: JsonObject? = null) {
        val payload = message.payload()
        assertEquals(expectedOp, payload["op"]?.jsonPrimitive?.content)
        assertEquals(after, payload["after"]?.takeUnless { it is JsonNull }?.jsonObject)
    }

    @Test
    fun `debezium captures full CDC lifecycle`() = runTest(timeout = 120.seconds) {
        // Initial (s)napshot
        executeSql(
            "CREATE TABLE IF NOT EXISTS test_items (id INT PRIMARY KEY, name TEXT)",
            "INSERT INTO test_items (id, name) VALUES (1, 'snapshot-row')",
        )

        registerConnectorAndAwait()

        // (c)reate, (u)pdate, (d)elete events
        executeSql(
            "INSERT INTO test_items (id, name) VALUES (2, 'inserted')",
            "UPDATE test_items SET name = 'updated' WHERE id = 2",
            "DELETE FROM test_items WHERE id = 2",
        )

        val messages = pollMessages("testdb.public.test_items", expected = 4)

        assertCdcEvent(messages[0], "r", after = buildJsonObject { put("id", 1); put("name", "snapshot-row") })
        assertCdcEvent(messages[1], "c", after = buildJsonObject { put("id", 2); put("name", "inserted") })
        assertCdcEvent(messages[2], "u", after = buildJsonObject { put("id", 2); put("name", "updated") })
        assertCdcEvent(messages[3], "d")
    }
}
