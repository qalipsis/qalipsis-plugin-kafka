/*
 * Copyright 2022 AERIS IT Solutions GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package io.qalipsis.plugins.kafka.producer

import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.impl.annotations.RelaxedMockK
import io.mockk.mockk
import io.mockk.spyk
import io.mockk.verifyOrder
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.context.StepName
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.meters.CampaignMeterRegistry
import io.qalipsis.api.meters.Counter
import io.qalipsis.api.meters.Meter
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.qalipsis.test.mockk.WithMockk
import io.qalipsis.test.mockk.relaxedMockk
import io.qalipsis.test.steps.StepTestHelper
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.Serdes
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension

/**
 * @author Gabriel Moraes
 */
@WithMockk
internal class KafkaProducerStepTest {

    @JvmField
    @RegisterExtension
    val testDispatcherProvider = TestDispatcherProvider()

    private lateinit var recordsBuilder: (suspend (ctx: StepContext<*, *>, input: Any) -> List<KafkaProducerRecord<String, String>>)

    private val keySerializer = Serdes.String().serializer()

    private val valueSerializer = Serdes.String().serializer()

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
    private lateinit var meterRegistry: CampaignMeterRegistry

    @Test
    fun `should produce without recording metrics`() = testDispatcherProvider.runTest {
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
                StepName(), null, "test", relaxedMockk(),
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
    fun `should produce recording metrics without record key`() = testDispatcherProvider.runTest {
        // given
        val metersTags: Map<String, String> = mockk()
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
        every {
            meterRegistry.counter(
                "scenario-name",
                "step-name",
                "kafka-produce-value-bytes",
                refEq(metersTags)
            )
        } returns valuesBytesSent
        every { valuesBytesSent.report(any()) } returns valuesBytesSent
        every {
            meterRegistry.counter(
                "scenario-name",
                "step-name",
                "kafka-produce-key-bytes",
                refEq(metersTags)
            )
        } returns keysBytesSent
        every { keysBytesSent.report(any()) } returns keysBytesSent
        every {
            meterRegistry.counter(
                "scenario-name",
                "step-name",
                "kafka-produce-records",
                refEq(metersTags)
            )
        } returns recordsCount
        every { recordsCount.report(any()) } returns recordsCount

        every { startStopContext.toMetersTags() } returns metersTags
        every { startStopContext.scenarioName } returns "scenario-name"
        every { startStopContext.stepName } returns "step-name"

        val kafkaProducerStep = spyk(
            KafkaProducerStep(
                StepName(), null, "test", relaxedMockk(),
                recordsBuilder, keySerializer, valueSerializer, eventsLogger, meterRegistry
            ), recordPrivateCalls = true
        )
        every { kafkaProducerStep["buildProducer"]() } returns relaxedMockk<KafkaProducer<*, *>>()

        val eventsTags: Map<String, String> = mockk()
        val context = spyk(StepTestHelper.createStepContext<Any, KafkaProducerResult<Any>>(input = "Any")) {
            every { toEventTags() } returns eventsTags
        }

        // when
        kafkaProducerStep.start(startStopContext)
        kafkaProducerStep.execute(context)

        //then
        verifyOrder {
            meterRegistry.counter("scenario-name", "step-name", "kafka-produce-records", refEq(metersTags))
            recordsCount.report(any<Meter.ReportingConfiguration<Counter>.() -> Unit>())
            meterRegistry.counter("scenario-name", "step-name", "kafka-produce-key-bytes", refEq(metersTags))
            keysBytesSent.report(any<Meter.ReportingConfiguration<Counter>.() -> Unit>())
            meterRegistry.counter("scenario-name", "step-name", "kafka-produce-value-bytes", refEq(metersTags))
            valuesBytesSent.report(any<Meter.ReportingConfiguration<Counter>.() -> Unit>())
            recordsCount.increment(3.0)
            keysBytesSent.increment(0.0)
            valuesBytesSent.increment(30.0)
            eventsLogger.info("kafka.produce.sent.records", 3, any(), tags = refEq(eventsTags))
            eventsLogger.info("kafka.produce.sent.keys-bytes", 0, any(), tags = refEq(eventsTags))
            eventsLogger.info("kafka.produce.sent.values-bytes", 30, any(), tags = refEq(eventsTags))
        }

        confirmVerified(recordsCount, keysBytesSent, valuesBytesSent, eventsLogger, meterRegistry)
    }

    @Test
    fun `should produce recording metrics with record key`() = testDispatcherProvider.runTest {
        // given
        val metersTags: Map<String, String> = emptyMap()
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
        every {
            meterRegistry.counter(
                "scenario-name",
                "step-name",
                "kafka-produce-value-bytes",
                refEq(metersTags)
            )
        } returns valuesBytesSent
        every { valuesBytesSent.report(any()) } returns valuesBytesSent
        every {
            meterRegistry.counter(
                "scenario-name",
                "step-name",
                "kafka-produce-key-bytes",
                refEq(metersTags)
            )
        } returns keysBytesSent
        every { keysBytesSent.report(any()) } returns keysBytesSent
        every {
            meterRegistry.counter(
                "scenario-name",
                "step-name",
                "kafka-produce-records",
                refEq(metersTags)
            )
        } returns recordsCount
        every { recordsCount.report(any()) } returns recordsCount

        every { startStopContext.toMetersTags() } returns metersTags
        every { startStopContext.scenarioName } returns "scenario-name"
        every { startStopContext.stepName } returns "step-name"

        val kafkaProducerStep = spyk(
            KafkaProducerStep(
                StepName(), null, "test", relaxedMockk(),
                recordsBuilder, keySerializer, valueSerializer, eventsLogger, meterRegistry
            ), recordPrivateCalls = true
        )

        every { kafkaProducerStep["buildProducer"]() } returns relaxedMockk<KafkaProducer<*, *>>()

        val eventsTags: Map<String, String> = mockk()
        val context = spyk(StepTestHelper.createStepContext<Any, KafkaProducerResult<Any>>(input = "Any")) {
            every { toEventTags() } returns eventsTags
        }

        // when
        kafkaProducerStep.start(startStopContext)
        kafkaProducerStep.execute(context)

        //then
        verifyOrder {
            meterRegistry.counter("scenario-name", "step-name", "kafka-produce-records", refEq(metersTags))
            recordsCount.report(any<Meter.ReportingConfiguration<Counter>.() -> Unit>())
            meterRegistry.counter("scenario-name", "step-name", "kafka-produce-key-bytes", refEq(metersTags))
            keysBytesSent.report(any<Meter.ReportingConfiguration<Counter>.() -> Unit>())
            meterRegistry.counter("scenario-name", "step-name", "kafka-produce-value-bytes", refEq(metersTags))
            valuesBytesSent.report(any<Meter.ReportingConfiguration<Counter>.() -> Unit>())
            recordsCount.increment(3.0)
            keysBytesSent.increment(eq(18.0))
            valuesBytesSent.increment(eq(30.0))
            eventsLogger.info("kafka.produce.sent.records", 3, any(), tags = refEq(eventsTags))
            eventsLogger.info("kafka.produce.sent.keys-bytes", 18, any(), tags = refEq(eventsTags))
            eventsLogger.info("kafka.produce.sent.values-bytes", 30, any(), tags = refEq(eventsTags))
        }

        confirmVerified(recordsCount, keysBytesSent, valuesBytesSent, eventsLogger, meterRegistry)
    }
}