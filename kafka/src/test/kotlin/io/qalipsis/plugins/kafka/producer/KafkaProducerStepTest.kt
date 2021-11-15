package io.qalipsis.plugins.kafka.producer

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.impl.annotations.RelaxedMockK
import io.mockk.spyk
import io.mockk.verifyOrder
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.context.StepId
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.test.mockk.WithMockk
import io.qalipsis.test.mockk.relaxedMockk
import io.qalipsis.test.steps.StepTestHelper
import kotlinx.coroutines.test.runBlockingTest
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.Serdes
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

/**
 * @author Gabriel Moraes
 */
@WithMockk
internal class KafkaProducerStepTest {

    private lateinit var recordsBuilder: (suspend (ctx: StepContext<*, *>, input: Any) -> List<KafkaProducerRecord<String, String>>)

    private val keySerializer = Serdes.String().serializer()

    private val valueSerializer = Serdes.String().serializer()

    @RelaxedMockK
    private lateinit var metersTags: Tags

    @RelaxedMockK
    private lateinit var startStopContext: StepStartStopContext

    @RelaxedMockK
    private lateinit var keysBytesSent: Counter

    @RelaxedMockK
    private lateinit var valuesBytesSent: Counter

    @RelaxedMockK
    private lateinit var recordsCount: Counter

    @RelaxedMockK
    private lateinit var eventsLogger: EventsLogger

    @RelaxedMockK
    private lateinit var meterRegistry: MeterRegistry

    @BeforeEach
    internal fun setUp() {
        every { startStopContext.toMetersTags() } returns metersTags
    }

    @Test
    fun `should produce without recording metrics`() = runBlockingTest {
        // given
        recordsBuilder = { _, _ ->
            listOf(
                KafkaProducerRecord(
                    topic = "payload",
                    value = "test"
                )
            )
        }

        val kafkaProducerStep = spyk(
            KafkaProducerStep(
                StepId(), null, "test", relaxedMockk(),
                recordsBuilder, keySerializer, valueSerializer, null, null
            ), recordPrivateCalls = true
        )
        every { kafkaProducerStep["buildProducer"]() } returns relaxedMockk<KafkaProducer<*, *>>()

        val context = StepTestHelper.createStepContext<Any, KafkaProducerResult<Any>>(input = "Any")

        // when
        kafkaProducerStep.start(startStopContext)
        kafkaProducerStep.execute(context)

        // then
        confirmVerified(recordsCount, keysBytesSent, valuesBytesSent, eventsLogger, meterRegistry)
    }

    @Test
    fun `should produce recording metrics without record key`() = runBlockingTest {
        // given
        recordsBuilder = { _, _ ->
            listOf(
                KafkaProducerRecord(
                    topic = "topic-1",
                    value = "payload-A"
                ),
                KafkaProducerRecord(
                    topic = "topic-2",
                    value = "payload-AA"
                ),
                KafkaProducerRecord(
                    topic = "topic-3",
                    value = "payload-AAA"
                )
            )
        }
        every { meterRegistry.counter("kafka-produce-keys-bytes", refEq(metersTags)) } returns keysBytesSent
        every { meterRegistry.counter("kafka-produce-values-bytes", refEq(metersTags)) } returns valuesBytesSent
        every { meterRegistry.counter("kafka-produce-records", refEq(metersTags)) } returns recordsCount

        val kafkaProducerStep = spyk(
            KafkaProducerStep(
                StepId(), null, "test", relaxedMockk(),
                recordsBuilder, keySerializer, valueSerializer, eventsLogger, meterRegistry
            ), recordPrivateCalls = true
        )
        every { kafkaProducerStep["buildProducer"]() } returns relaxedMockk<KafkaProducer<*, *>>()

        val eventTags = relaxedMockk<Map<String, String>>()
        val context = spyk(StepTestHelper.createStepContext<Any, KafkaProducerResult<Any>>(input = "Any")) {
            every { toEventTags() } returns eventTags
        }

        // when
        kafkaProducerStep.start(startStopContext)
        kafkaProducerStep.execute(context)

        //then
        verifyOrder {
            meterRegistry.counter("kafka-produce-records", refEq(metersTags))
            meterRegistry.counter("kafka-produce-keys-bytes", refEq(metersTags))
            meterRegistry.counter("kafka-produce-values-bytes", refEq(metersTags))
            recordsCount.increment(3.0)
            keysBytesSent.increment(0.0)
            valuesBytesSent.increment(30.0)
            eventsLogger.info("kafka.produce.sent.records", 3, any(), tags = refEq(eventTags))
            eventsLogger.info("kafka.produce.sent.keys-bytes", 0, any(), tags = refEq(eventTags))
            eventsLogger.info("kafka.produce.sent.values-bytes", 30, any(), tags = refEq(eventTags))
        }

        confirmVerified(recordsCount, keysBytesSent, valuesBytesSent, eventsLogger, meterRegistry)
    }

    @Test
    fun `should produce recording metrics with record key`() = runBlockingTest {
        // given
        recordsBuilder = { _, _ ->
            listOf(
                KafkaProducerRecord(
                    topic = "topic-1",
                    key = "key-A",
                    value = "payload-A"
                ),
                KafkaProducerRecord(
                    topic = "topic-2",
                    key = "key-AA",
                    value = "payload-AA"
                ),
                KafkaProducerRecord(
                    topic = "topic-3",
                    key = "key-AAA",
                    value = "payload-AAA"
                )
            )
        }
        every { meterRegistry.counter("kafka-produce-keys-bytes", refEq(metersTags)) } returns keysBytesSent
        every { meterRegistry.counter("kafka-produce-values-bytes", refEq(metersTags)) } returns valuesBytesSent
        every { meterRegistry.counter("kafka-produce-records", refEq(metersTags)) } returns recordsCount

        val kafkaProducerStep = spyk(
            KafkaProducerStep(
                StepId(), null, "test", relaxedMockk(),
                recordsBuilder, keySerializer, valueSerializer, eventsLogger, meterRegistry
            ), recordPrivateCalls = true
        )

        every { kafkaProducerStep["buildProducer"]() } returns relaxedMockk<KafkaProducer<*, *>>()
        val eventTags = relaxedMockk<Map<String, String>>()
        val context = spyk(StepTestHelper.createStepContext<Any, KafkaProducerResult<Any>>(input = "Any")) {
            every { toEventTags() } returns eventTags
        }

        // when
        kafkaProducerStep.start(startStopContext)
        kafkaProducerStep.execute(context)

        //then
        verifyOrder {
            meterRegistry.counter("kafka-produce-records", refEq(metersTags))
            meterRegistry.counter("kafka-produce-keys-bytes", refEq(metersTags))
            meterRegistry.counter("kafka-produce-values-bytes", refEq(metersTags))
            recordsCount.increment(3.0)
            keysBytesSent.increment(eq(18.0))
            valuesBytesSent.increment(eq(30.0))
            eventsLogger.info("kafka.produce.sent.records", 3, any(), tags = refEq(eventTags))
            eventsLogger.info("kafka.produce.sent.keys-bytes", 18, any(), tags = refEq(eventTags))
            eventsLogger.info("kafka.produce.sent.values-bytes", 30, any(), tags = refEq(eventTags))
        }

        confirmVerified(recordsCount, keysBytesSent, valuesBytesSent, eventsLogger, meterRegistry)
    }
}