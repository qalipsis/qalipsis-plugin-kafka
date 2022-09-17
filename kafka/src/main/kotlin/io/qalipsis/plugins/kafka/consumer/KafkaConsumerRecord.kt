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