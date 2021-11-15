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