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

package io.qalipsis.plugins.kafka.producer

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.header.internals.RecordHeaders

/**
 * Qalipsis representation of a Kafka record to produce.
 *
 * @property topic, name of the topic to route the message to.
 * @property partition of the topic, defaults to null.
 * @property headers of the produced message, defaults to empty map.
 * @property key of the message, defaults to null.
 * @property value of the message.
 *
 * @author Eric Jessé
 * @author Gabriel Moraes
 */
data class KafkaProducerRecord<K, V>(
    val topic: String,
    val partition: Int? = null,
    val headers: Map<String, ByteArray> = emptyMap(),
    val key: K? = null,
    val value: V
) {

    fun toProducerRecord(): ProducerRecord<K, V> {
        return ProducerRecord(topic, partition, key, value,
            RecordHeaders(headers.map { RecordHeader(it.key, it.value) }.toList())
        )
    }
}