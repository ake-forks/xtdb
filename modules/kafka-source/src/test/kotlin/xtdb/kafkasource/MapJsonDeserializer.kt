package xtdb.kafkasource

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.longOrNull
import org.apache.kafka.common.serialization.Deserializer
import java.time.Instant

/**
 * Test-only deserializer: JSON object bytes → `Map<String, Any?>` (or null on tombstones).
 *
 * The `!Docs` indexer expects map-shaped values; the standard Kafka Connect /
 * Confluent JSON deserializers produce JsonNode objects. This is the smallest
 * thing that gets us a Map and works under any K-side serializer combo.
 *
 * Special-case: `_valid_from` and `_valid_to` ISO-8601 strings are parsed to
 * [Instant]s so the indexer's `as? Instant` checks pick them up.
 */
class MapJsonDeserializer : Deserializer<Map<String, Any?>?> {
    override fun deserialize(topic: String?, data: ByteArray?): Map<String, Any?>? {
        if (data == null) return null
        return when (val element = Json.parseToJsonElement(String(data, Charsets.UTF_8))) {
            is JsonObject -> element.entries.associate { (k, v) -> k to decodeAt(k, v) }
            else -> throw IllegalArgumentException(
                "expected a JSON object at the top level, got ${element::class.simpleName}",
            )
        }
    }
}

private fun decodeAt(key: String, el: JsonElement): Any? =
    if ((key == "_valid_from" || key == "_valid_to") && el is JsonPrimitive && el.isString)
        Instant.parse(el.content)
    else decode(el)

private fun decode(el: JsonElement): Any? = when (el) {
    is JsonNull -> null
    is JsonPrimitive ->
        if (el.isString) el.content
        else el.booleanOrNull ?: el.longOrNull ?: el.doubleOrNull ?: el.content
    is JsonArray -> el.map { decode(it) }
    is JsonObject -> el.entries.associate { (k, v) -> k to decode(v) }
}
