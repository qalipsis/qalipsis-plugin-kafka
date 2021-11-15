package io.qalipsis.plugins.kafka.consumer

class KafkaConsumerResults<K, V>(
    val records: List<KafkaConsumerRecord<K, V>>,
    val meters: KafkaConsumerMeters
) : Iterable<KafkaConsumerRecord<K, V>> {

    override fun iterator(): Iterator<KafkaConsumerRecord<K, V>> {
        return records.iterator()
    }
}