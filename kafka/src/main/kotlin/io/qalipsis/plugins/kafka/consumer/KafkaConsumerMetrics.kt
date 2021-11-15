package io.qalipsis.plugins.kafka.consumer

import io.micrometer.core.instrument.Counter

/**
 * Records the metrics for the Kafka producer.
 *
 * @property consumedKeyBytesCounter records the number of bytes consumed for the serialized keys.
 * @property consumedValueBytesCounter records the number of bytes consumed for the serialized values.
 * @property recordconsumedRecordsCountersCount counts the number of records consumed.
 *
 * @author Alex Averyanov
 */
internal class KafkaConsumerMetrics(
    var consumedKeyBytesCounter: Counter? = null,
    var consumedValueBytesCounter: Counter? = null,
    var consumedRecordsCounter: Counter? = null
) {

    fun countKeyBytes(size: Int) {
        consumedKeyBytesCounter?.increment(size.coerceAtLeast(0).toDouble())
    }

    fun countValueBytes(size: Int) {
        consumedValueBytesCounter?.increment(size.coerceAtLeast(0).toDouble())
    }

    fun countRecords() {
        consumedRecordsCounter?.increment()
    }
}