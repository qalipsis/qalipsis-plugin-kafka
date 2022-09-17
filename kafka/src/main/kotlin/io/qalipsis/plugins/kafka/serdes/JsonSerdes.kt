/*
 * Copyright 2022 AERIS IT Solutions GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

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

