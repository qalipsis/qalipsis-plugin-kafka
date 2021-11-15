package io.qalipsis.plugins.kafka.producer

/**
 * Records the metrics for the Kafka producer.
 *
 * @property keysBytesSent records the number of bytes sent for the serialized keys.
 * @property valuesBytesSent records the number of bytes sent for the serialized values.
 * @property recordsCount counts the number of records sent.
 *
 * @author Alex Averyanov
 */
data class KafkaProducerMeters(
    var keysBytesSent: Int = 0,
    var valuesBytesSent: Int = 0,
    var recordsCount: Int = 0,
)