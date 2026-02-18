package xtdb.debezium

import kotlinx.serialization.json.*
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.api.Xtdb
import xtdb.tx.TxOp
import java.time.Instant

class DebeziumConsumerTest {

    private lateinit var node: Xtdb
    private lateinit var consumer: DebeziumConsumer

    @BeforeEach
    fun setUp() {
        node = Xtdb.openNode { server { port = 0 }; flightSql = null }
        consumer = DebeziumConsumer(
            node = node,
            dbName = "xtdb",
            allocator = node.allocator,
            bootstrapServers = "unused:9092",
            topic = "unused",
            groupId = "unused",
        )
    }

    @AfterEach
    fun tearDown() {
        consumer.close()
        node.close()
    }

    private fun cdcPayload(
        op: String,
        before: JsonObject? = null,
        after: JsonObject? = null,
        schema: String = "public",
        table: String = "test_items",
        tsNs: Long = 1700000000000000000L,
        lsn: Long = 12345L,
    ): JsonObject = buildJsonObject {
        put("op", op)
        if (before != null) put("before", before) else put("before", JsonNull)
        if (after != null) put("after", after) else put("after", JsonNull)
        putJsonObject("source") {
            put("schema", schema)
            put("table", table)
            put("ts_ns", tsNs)
            put("lsn", lsn)
        }
    }

    private fun source(
        schema: String = "public",
        table: String = "test_items",
        tsNs: Long = 1700000000000000000L,
        lsn: Long = 12345L,
    ): JsonObject = buildJsonObject {
        put("schema", schema)
        put("table", table)
        put("ts_ns", tsNs)
        put("lsn", lsn)
    }

    // -- JSON conversion tests --

    @Test
    fun `jsonObjectToMap converts primitives correctly`() {
        val obj = buildJsonObject {
            put("str", "hello")
            put("int", 42)
            put("long", 9999999999L)
            put("double", 3.14)
            put("bool", true)
            put("nil", JsonNull)
        }
        val map = jsonObjectToMap(obj)

        assertEquals("hello", map["str"])
        assertEquals(42L, map["int"])
        assertEquals(9999999999L, map["long"])
        assertEquals(3.14, map["double"])
        assertEquals(true, map["bool"])
        assertNull(map["nil"])
    }

    @Test
    fun `tsNsToInstant converts nanoseconds to Instant`() {
        val src = buildJsonObject { put("ts_ns", 1700000000123456789L) }
        assertEquals(Instant.ofEpochSecond(1700000000, 123456789), tsNsToInstant(src))
    }

    @Test
    fun `tsNsToInstant returns null when source is null`() {
        assertNull(tsNsToInstant(null))
    }

    @Test
    fun `tsNsToInstant returns null when ts_ns missing`() {
        assertNull(tsNsToInstant(buildJsonObject { put("schema", "public") }))
    }

    // -- Translation tests via translateToTxOp --

    @Test
    fun `create op produces PutDocs with correct table and fields`() {
        val after = buildJsonObject { put("_id", 1); put("name", "Alice") }
        val payload = cdcPayload("c", after = after)
        val source = payload["source"]!!.jsonObject

        consumer.translateToTxOp("c", payload, source).use { op ->
            assertInstanceOf(TxOp.PutDocs::class.java, op)
            op as TxOp.PutDocs
            assertEquals("public", op.schema)
            assertEquals("test_items", op.table)
            assertEquals(Instant.ofEpochSecond(1700000000, 0), op.validFrom)
            assertNull(op.validTo)
            assertEquals(1, op.docs.rowCount)
        }
    }

    @Test
    fun `read op produces PutDocs same as create`() {
        val after = buildJsonObject { put("_id", 1); put("name", "snapshot") }
        val payload = cdcPayload("r", after = after)
        val source = payload["source"]!!.jsonObject

        consumer.translateToTxOp("r", payload, source).use { op ->
            assertInstanceOf(TxOp.PutDocs::class.java, op)
        }
    }

    @Test
    fun `update op produces PutDocs with updated fields`() {
        val after = buildJsonObject { put("_id", 2); put("name", "updated"); put("score", 99) }
        val payload = cdcPayload("u", after = after)
        val source = payload["source"]!!.jsonObject

        consumer.translateToTxOp("u", payload, source).use { op ->
            assertInstanceOf(TxOp.PutDocs::class.java, op)
            op as TxOp.PutDocs
            assertEquals("test_items", op.table)
            assertEquals(1, op.docs.rowCount)
        }
    }

    @Test
    fun `delete op produces DeleteDocs with correct id`() {
        val before = buildJsonObject { put("_id", 3); put("name", "to-delete") }
        val payload = cdcPayload("d", before = before)
        val source = payload["source"]!!.jsonObject

        consumer.translateToTxOp("d", payload, source).use { op ->
            assertInstanceOf(TxOp.DeleteDocs::class.java, op)
            op as TxOp.DeleteDocs
            assertEquals("test_items", op.table)
            assertEquals(1, op.ids.valueCount)
        }
    }

    @Test
    fun `missing _id in after throws`() {
        val after = buildJsonObject { put("name", "no-id") }
        val payload = cdcPayload("c", after = after)
        val source = payload["source"]!!.jsonObject

        val ex = assertThrows(IllegalArgumentException::class.java) {
            consumer.translateToTxOp("c", payload, source)
        }
        assertTrue(ex.message!!.contains("_id"))
    }

    @Test
    fun `delete with null before throws`() {
        val payload = cdcPayload("d")
        val source = payload["source"]!!.jsonObject

        val ex = assertThrows(IllegalArgumentException::class.java) {
            consumer.translateToTxOp("d", payload, source)
        }
        assertTrue(ex.message!!.contains("before"))
    }

    @Test
    fun `_valid_from in doc is extracted and set on op`() {
        val after = buildJsonObject {
            put("_id", 1); put("name", "timed"); put("_valid_from", "2024-01-01T00:00:00Z")
        }
        val payload = cdcPayload("c", after = after)
        val source = payload["source"]!!.jsonObject

        consumer.translateToTxOp("c", payload, source).use { op ->
            op as TxOp.PutDocs
            assertEquals(Instant.parse("2024-01-01T00:00:00Z"), op.validFrom)
        }
    }

    @Test
    fun `_valid_from absent defaults to source ts_ns`() {
        val after = buildJsonObject { put("_id", 1); put("name", "no-vf") }
        val tsNs = 1700000000500000000L
        val payload = cdcPayload("c", after = after, tsNs = tsNs)
        val source = payload["source"]!!.jsonObject

        consumer.translateToTxOp("c", payload, source).use { op ->
            op as TxOp.PutDocs
            assertEquals(Instant.ofEpochSecond(1700000000, 500000000), op.validFrom)
        }
    }

    @Test
    fun `_valid_to in doc is extracted and set on op`() {
        val after = buildJsonObject {
            put("_id", 1); put("name", "bounded")
            put("_valid_from", "2024-01-01T00:00:00Z")
            put("_valid_to", "2025-01-01T00:00:00Z")
        }
        val payload = cdcPayload("c", after = after)
        val source = payload["source"]!!.jsonObject

        consumer.translateToTxOp("c", payload, source).use { op ->
            op as TxOp.PutDocs
            assertEquals(Instant.parse("2024-01-01T00:00:00Z"), op.validFrom)
            assertEquals(Instant.parse("2025-01-01T00:00:00Z"), op.validTo)
        }
    }

    @Test
    fun `schema and table come from source`() {
        val after = buildJsonObject { put("_id", 1) }
        val payload = cdcPayload("c", after = after, schema = "myschema", table = "users")
        val source = payload["source"]!!.jsonObject

        consumer.translateToTxOp("c", payload, source).use { op ->
            op as TxOp.PutDocs
            assertEquals("myschema", op.schema)
            assertEquals("users", op.table)
        }
    }
}
