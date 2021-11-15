package io.qalipsis.plugins.kafka.serdes

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer
import kotlin.reflect.KClass

/**
 * Kafka serializer and deserializer for JSON
 *
 * @author Eric Jessé
 */
class JsonSerdes<T>(
    type: KClass<*>,
    private val objectMapper: ObjectMapper = DefaultJsonMapper
) : Serializer<T>, Deserializer<T> {

    private val javaType = type.java

    override fun serialize(topic: String, data: T): ByteArray {
        return objectMapper.writeValueAsBytes(data)
    }

    @Suppress("UNCHECKED_CAST")
    override fun deserialize(topic: String, data: ByteArray): T {
        return objectMapper.readValue(data, javaType) as T
    }

    override fun close() = Unit

    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) = Unit

}

/**
 * Implementation of Kafka [Serde] to provide ready to use [Serializer]s and [Deserializer]s.
 *
 * @author Eric Jessé
 */
internal class JsonSerde<T : Any>(private val type: KClass<T>, private val objectMapper: ObjectMapper) : Serde<T> {

    override fun serializer(): Serializer<T> = JsonSerdes(type, objectMapper)

    override fun deserializer(): Deserializer<T> = JsonSerdes(type, objectMapper)
}

/**
 * Provides a Kafka [Serde] to work with JSON.
 *
 * @author Eric Jessé
 */
fun <U : Any> jsonSerde(type: KClass<U>, objectMapper: ObjectMapper? = null): Serde<U> =
    JsonSerde(type, objectMapper ?: DefaultJsonMapper)

/**
 * Provides a Kafka [Serde] to work with JSON.
 *
 * @author Eric Jessé
 */
inline fun <reified T : Any> jsonSerde(objectMapper: ObjectMapper? = null): Serde<T> = jsonSerde(T::class, objectMapper)

