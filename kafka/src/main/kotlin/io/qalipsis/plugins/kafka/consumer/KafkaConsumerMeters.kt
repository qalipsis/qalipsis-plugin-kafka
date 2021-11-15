package io.qalipsis.plugins.kafka.consumer

/**
 * Records the metrics for the Kafka consumer.
 *
 * @property keysBytesReceived records the number of bytes sent for the serialized keys.
 * @property valuesBytesReceived records the number of bytes sent for the serialized values.
 * @property recordsCount counts the number of records sent.
 *
 * @author Alex Averyanov
 */
data class KafkaConsumerMeters(
    var keysBytesReceived: Int = 0,
    var valuesBytesReceived: Int = 0,
    var recordsCount: Int = 0
)