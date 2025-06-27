/*
 * QALIPSIS
 * Copyright (C) 2025 AERIS IT Solutions GmbH
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
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