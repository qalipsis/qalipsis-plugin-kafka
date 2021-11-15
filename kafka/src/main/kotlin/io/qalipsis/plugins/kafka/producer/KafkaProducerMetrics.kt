package io.qalipsis.plugins.kafka.producer

import io.micrometer.core.instrument.Counter

/**
 * Records the metrics for the Kafka producer.
 *
 * @property keysBytesSent records the number of bytes sent for the serialized keys.
 * @property valuesBytesSent records the number of bytes sent for the serialized values.
 * @property recordsCount counts the number of records sent.
 */
internal class KafkaProducerMetrics(
    var keysBytesSent: Counter? = null,
    var valuesBytesSent: Counter? = null,
    var recordsCount: Counter? = null
)