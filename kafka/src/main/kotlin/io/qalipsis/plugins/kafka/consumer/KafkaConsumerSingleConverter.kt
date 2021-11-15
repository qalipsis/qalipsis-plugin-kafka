package io.qalipsis.plugins.kafka.consumer

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.qalipsis.api.context.StepOutput
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.lang.tryAndLogOrNull
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.steps.datasource.DatasourceObjectConverter
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.common.serialization.Deserializer
import java.util.concurrent.atomic.AtomicLong

/**
 * Implementation of [DatasourceObjectConverter], that reads a batch of native Kafka records and forwards each of
 * them converted to as [KafkaConsumerRecord].
 *
 * @author Eric Jessé
 */
internal class KafkaConsumerSingleConverter<K, V>(
    private val keyDeserializer: Deserializer<K>,
    private val valueDeserializer: Deserializer<V>,
    private val meterRegistry: MeterRegistry?,
    private val eventsLogger: EventsLogger?

) : DatasourceObjectConverter<ConsumerRecords<ByteArray?, ByteArray?>, KafkaConsumerResult<K, V>> {

    private var consumedKeyBytesCounter: Counter? = null

    private var consumedValueBytesCounter: Counter? = null

    private var consumedRecordsCounter: Counter? = null

    private val eventPrefix: String = "kafka.consume"

    private val meterPrefix: String = "kafka-consume"

    private lateinit var eventTags: Map<String, String>


    override fun start(context: StepStartStopContext) {
        meterRegistry?.apply {
            val tags = context.toMetersTags()
            consumedKeyBytesCounter = counter("$meterPrefix-key-bytes", tags)
            consumedValueBytesCounter = counter("$meterPrefix-value-bytes", tags)
            consumedRecordsCounter = counter("$meterPrefix-records", tags)
        }
        eventTags = context.toEventTags();
    }


    override fun stop(context: StepStartStopContext) {
        meterRegistry?.apply {
            remove(consumedKeyBytesCounter!!)
            remove(consumedValueBytesCounter!!)
            remove(consumedRecordsCounter!!)
            consumedKeyBytesCounter = null
            consumedValueBytesCounter = null
            consumedRecordsCounter = null
        }
    }

    override suspend fun supply(
        offset: AtomicLong, value: ConsumerRecords<ByteArray?, ByteArray?>,
        output: StepOutput<KafkaConsumerResult<K, V>>
    ) {
        val kafkaConsumerMeters = KafkaConsumerMeters(recordsCount = value.count())

        value.forEach { record ->
            val keySize = record.serializedKeySize()
            if (keySize > 0) {
                kafkaConsumerMeters.keysBytesReceived += keySize
            }
            val valueSize = record.serializedValueSize()
            if (valueSize > 0) {
                kafkaConsumerMeters.valuesBytesReceived += valueSize
            }
            tryAndLogOrNull(log) {
                output.send(
                    KafkaConsumerResult(
                        KafkaConsumerRecord(
                            offset.getAndIncrement(),
                            record,
                            keyDeserializer.deserialize(record.topic(), record.headers(), record.key()),
                            valueDeserializer.deserialize(record.topic(), record.headers(), record.value())
                        ),
                        KafkaConsumerMeters(
                            keySize,
                            valueSize,
                            value.count()
                        )
                    )
                )
            }
        }
        consumedRecordsCounter?.increment(value.count().toDouble())
        consumedKeyBytesCounter?.increment(kafkaConsumerMeters.keysBytesReceived.toDouble())
        consumedValueBytesCounter?.increment(kafkaConsumerMeters.valuesBytesReceived.toDouble())

        eventsLogger?.info("${eventPrefix}.consumed.records", kafkaConsumerMeters.recordsCount, tags = eventTags)
        eventsLogger?.info("${eventPrefix}.consumed.key-bytes", kafkaConsumerMeters.keysBytesReceived, tags = eventTags)
        eventsLogger?.info(
            "${eventPrefix}.consumed.value-bytes",
            kafkaConsumerMeters.valuesBytesReceived,
            tags = eventTags
        )
    }

    companion object {

        @JvmStatic
        private val log = logger()
    }
}