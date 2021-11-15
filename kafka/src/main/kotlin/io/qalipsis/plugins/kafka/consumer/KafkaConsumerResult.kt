package io.qalipsis.plugins.kafka.consumer

class KafkaConsumerResult<K, V>(
    val record: KafkaConsumerRecord<K, V>,
    val meters: KafkaConsumerMeters
)