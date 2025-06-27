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

package io.qalipsis.plugins.kafka.consumer

import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * Qalipsis representation of a consumed Kafka record.
 *
 * @author Eric Jessé
 *
 * @property topic name of the topic from where the record was consumed
 * @property partition topic partition from where the record was consumed
 * @property offset record offset as provided by Kafka
 * @property receivedTimestamp received timestamp as provided by Kafka
 * @property consumedOffset offset of the record relatively to the Qalipsis consumer
 * @property consumedTimestamp timestamp when the message was consumed by Qalipsis
 * @property headers headers of
 */
data class KafkaConsumerRecord<K, V>(
        val topic: String,
        val partition: Int,
        val offset: Long,
        val receivedTimestamp: Long,
        val consumedOffset: Long,
        val consumedTimestamp: Long,
        val headers: Map<String, ByteArray>,
        val key: K,
        val value: V
) {
    internal constructor(
        consumedOffset: Long, record: ConsumerRecord<*, *>,
        key: K, value: V
    ) : this(
        topic = record.topic(),
        partition = record.partition(),
        offset = record.offset(),
        receivedTimestamp = record.timestamp(),
        consumedOffset = consumedOffset,
        consumedTimestamp = System.currentTimeMillis(),
        headers = record.headers().toArray().map { it.key() to it.value() }.toMap(),
        key = key,
        value = value
    )
}