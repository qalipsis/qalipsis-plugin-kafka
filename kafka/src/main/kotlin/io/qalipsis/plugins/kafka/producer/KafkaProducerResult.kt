package io.qalipsis.plugins.kafka.producer

/**
 * Qalispsis representation of a Kafka Producer result.
 *
 * @property input from the previous steps output.
 *
 * @author Gabriel Moraes
 */
data class KafkaProducerResult<I>(
    val input: I,
    val metrics: KafkaProducerMeters
)