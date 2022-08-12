package io.qalipsis.plugins.kafka.meters

import io.micrometer.core.instrument.Clock
import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.step.StepMeterRegistry
import io.qalipsis.api.lang.tryAndLogOrNull
import io.qalipsis.api.logging.LoggerHelper.logger
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serdes
import java.time.Duration
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit

/**
 * [io.micrometer.core.instrument.MeterRegistry] for Apache Kafka.
 *
 * @author Palina Bril
 */
internal class KafkaMeterRegistry(
    private val config: KafkaMeterConfig,
    clock: Clock
) : StepMeterRegistry(config, clock) {

    private lateinit var producer: Producer<ByteArray, Meter>

    private val topic = config.topic()

    override fun start(threadFactory: ThreadFactory) {
        producer = KafkaProducer(
            config.configuration(),
            Serdes.ByteArray().serializer(),
            JsonMeterSerializer(this, baseTimeUnit, config.timestampFieldName())
        )
        super.start(threadFactory)
    }

    override fun stop() {
        tryAndLogOrNull(log) {
            producer.close(Duration.ofSeconds(10))
        }
    }

    override fun getBaseTimeUnit(): TimeUnit {
        return TimeUnit.MILLISECONDS
    }

    public override fun publish() {
        try {
            meters.stream().forEach { producer.send(ProducerRecord(topic, it)) }
            log.debug { "Successfully sent ${meters.size} meters to Kafka" }
        } catch (e: Throwable) {
            log.error(e) { "Failed to send metrics to Kafka" }
        }
    }

    fun getName(meter: Meter): String {
        return getConventionName(meter.id)
    }

    fun getTags(meter: Meter): MutableList<Tag> {
        return getConventionTags(meter.id)
    }

    private companion object {

        val log = logger()
    }
}
