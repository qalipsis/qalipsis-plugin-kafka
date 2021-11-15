package io.qalipsis.plugins.kafka.events

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micronaut.context.annotation.Requires
import io.qalipsis.api.events.Event
import io.qalipsis.api.events.EventJsonConverter
import io.qalipsis.api.events.EventsPublisher
import io.qalipsis.api.lang.tryAndLogOrNull
import io.qalipsis.api.logging.LoggerHelper.logger
import jakarta.inject.Singleton
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serdes
import java.time.Duration
import java.util.Properties

/**
 * Implementation of [io.qalipsis.api.events.EventsLogger] for Kafka.
 * The structure of the data is similar to the one in Elasticsearch to allow asynchronous via Logstash.
 * However, this does not prevent from
 *
 * @author Eric Jessé
 */
@Singleton
@Requires(beans = [KafkaEventsConfiguration::class])
internal class KafkaEventsPublisher(
    private val configuration: KafkaEventsConfiguration,
    private val meterRegistry: MeterRegistry,
    private val eventsConverter: EventJsonConverter
) : EventsPublisher {

    private lateinit var producer: Producer<ByteArray, Event>

    private lateinit var counter: Counter

    override fun start() {
        val actualProperties = Properties()
        actualProperties.putAll(configuration.configuration)
        actualProperties[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = configuration.bootstrap
        actualProperties[ProducerConfig.LINGER_MS_CONFIG] = configuration.lingerPeriod.toMillis().toString()
        actualProperties[ProducerConfig.BATCH_SIZE_CONFIG] = configuration.batchSize.toString()
        producer =
            KafkaProducer(actualProperties, Serdes.ByteArray().serializer(), JsonEventSerializer(eventsConverter))
        counter = meterRegistry.counter(EVENTS_EXPORT_TIMER_NAME)
        super.start()
    }

    override fun stop() {
        tryAndLogOrNull(log) {
            producer.close(Duration.ofSeconds(10))
        }
        meterRegistry.remove(counter)
    }

    override fun publish(event: Event) {
        producer.send(ProducerRecord(configuration.topic, event))
        counter.increment()
    }

    companion object {

        private const val EVENTS_EXPORT_TIMER_NAME = "kafka.events.export"

        @JvmStatic
        private val log = logger()
    }

}
