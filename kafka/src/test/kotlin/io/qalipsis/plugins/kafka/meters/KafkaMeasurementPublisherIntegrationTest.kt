/*
 * Copyright 2024 AERIS IT Solutions GmbH
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

package io.qalipsis.plugins.kafka.meters

import assertk.all
import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.isEqualTo
import assertk.assertions.isNotNull
import assertk.assertions.key
import assertk.assertions.prop
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.PropertySource
import io.micronaut.context.env.PropertySource as EnvPropertySource
import io.micronaut.context.env.Environment
import io.micronaut.core.util.StringUtils
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.every
import io.mockk.impl.annotations.RelaxedMockK
import io.mockk.mockk
import io.mockk.mockkStatic
import io.qalipsis.api.meters.DistributionMeasurementMetric
import io.qalipsis.api.meters.DistributionSummary
import io.qalipsis.api.meters.MeasurementMetric
import io.qalipsis.api.meters.MeterSnapshot as QalipsisMeterSnapshot
import io.qalipsis.api.meters.MeterType
import io.qalipsis.api.meters.Statistic
import io.qalipsis.plugins.kafka.Constants
import io.qalipsis.plugins.kafka.config.KafkaMeasurementPublisherFactory
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.qalipsis.test.mockk.WithMockk
import jakarta.inject.Inject
import kotlinx.coroutines.CoroutineScope
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serdes
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.extension.RegisterExtension
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.util.Properties
import kotlin.math.pow

@Testcontainers
@MicronautTest(startApplication = false)
@PropertySource(
    Property(name = "meters.export.enabled", value = StringUtils.TRUE),
    Property(name = "meters.export.kafka.enabled", value = StringUtils.TRUE)
)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@WithMockk
internal class KafkaMeasurementPublisherIntegrationTest {

    @Inject
    private lateinit var applicationContext: ApplicationContext

    @JvmField
    @RegisterExtension
    val testDispatcherProvider = TestDispatcherProvider()

    private lateinit var bootstrapServers: String

    private lateinit var consumer: KafkaConsumer<ByteArray, MeterSnapshot>

    private lateinit var configuration: KafkaMeterConfig

    @RelaxedMockK
    private lateinit var coroutineScope: CoroutineScope

    @Inject
    private lateinit var measurementPublisherFactory: KafkaMeasurementPublisherFactory

    private fun getProperties(): Properties {
        val consumerProperties = Properties()
        bootstrapServers = CONTAINER.bootstrapServers.substringAfter("PLAINTEXT://")
        consumerProperties["kafka.bootstrap.servers"] = "kafka-1:9092"
        consumerProperties[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        consumerProperties[ConsumerConfig.GROUP_ID_CONFIG] = "test"
        consumerProperties[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "${OffsetResetStrategy.EARLIEST}".lowercase()
        consumerProperties[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = false
        consumerProperties["meters.export.kafka.bootstrap.servers"] = bootstrapServers

        return consumerProperties
    }

    @BeforeAll
    internal fun setUpAll() {
        consumer = KafkaConsumer(getProperties(), Serdes.ByteArray().deserializer(), JacksonMeterDeserializer())
        val environment = applicationContext.getBean(Environment::class.java)
        val customPropertySource = EnvPropertySource.of(
            "customSource", mapOf(
                "meters.export.kafka.bootstrap.servers" to bootstrapServers
            )
        )
        environment.addPropertySource(customPropertySource)
        configuration = KafkaMeterConfig(environment)
    }

    @AfterEach
    internal fun tearDown() {
        consumer.unsubscribe()
    }

    @AfterAll
    internal fun tearDownAll() {
        consumer.close()
    }

    @Test
    @Timeout(30)
    internal fun `should export data`() = testDispatcherProvider.run {
        // given
        val measurementPublisher = measurementPublisherFactory.getPublisher() as KafkaMeasurementPublisher
        measurementPublisher.init()
        val counterMock = mockk<io.qalipsis.api.meters.Counter> {
            every { id } returns mockk<io.qalipsis.api.meters.Meter.Id> {
                every { meterName } returns "the-counter"
                every { tags } returns mapOf("tag-1" to "value-1")
                every { type } returns MeterType.COUNTER
                every { scenarioName } returns "SCENARIO one"
                every { campaignKey } returns "campaign-1"
                every { stepName } returns "step uno"
            }
        }
        val timerMock = mockk<io.qalipsis.api.meters.Timer> {
            every { id } returns mockk<io.qalipsis.api.meters.Meter.Id> {
                every { meterName } returns "the-timer"
                every { tags } returns mapOf("tag-2" to "value-2")
                every { type } returns MeterType.TIMER
                every { scenarioName } returns "SCENARIO two"
                every { campaignKey } returns "campaign-1"
                every { stepName } returns "step dos"
            }
        }
        val gaugeMock = mockk<io.qalipsis.api.meters.Gauge> {
            every { id } returns mockk<io.qalipsis.api.meters.Meter.Id> {
                every { meterName } returns "the-gauge"
                every { tags } returns mapOf("tag-3" to "value-3")
                every { type } returns MeterType.GAUGE
                every { scenarioName } returns "SCENARIO-three"
                every { campaignKey } returns "campaign-1"
                every { stepName } returns "step tres"
            }
        }
        val summaryMock = mockk<DistributionSummary> {
            every { id } returns mockk<io.qalipsis.api.meters.Meter.Id> {
                every { meterName } returns "the-summary"
                every { tags } returns mapOf("tag-4" to "value-4")
                every { type } returns MeterType.DISTRIBUTION_SUMMARY
                every { scenarioName } returns "scenario four"
                every { campaignKey } returns "campaign-1"
                every { stepName } returns "step quart"
            }
        }
        val now = getTimeMock()
        val meterSnapshots = listOf(mockk<QalipsisMeterSnapshot<io.qalipsis.api.meters.Counter>> {
            every { timestamp } returns now
            every { meter } returns counterMock
            every { measurements } returns listOf(MeasurementMetric(8.0, Statistic.COUNT))
        }, mockk<QalipsisMeterSnapshot<io.qalipsis.api.meters.Gauge>> {
            every { timestamp } returns now
            every { meter } returns gaugeMock
            every { measurements } returns listOf(MeasurementMetric(654.0, Statistic.VALUE))
        }, mockk<QalipsisMeterSnapshot<io.qalipsis.api.meters.Timer>> {
            every { timestamp } returns now
            every { meter } returns timerMock
            every { measurements } returns listOf(
                MeasurementMetric(2.0, Statistic.MEAN),
                MeasurementMetric(40000.0, Statistic.TOTAL_TIME),
                MeasurementMetric(2000.0, Statistic.MAX),
                DistributionMeasurementMetric(500000448.5, Statistic.PERCENTILE, 85.0),
                DistributionMeasurementMetric(5432844.5, Statistic.PERCENTILE, 50.0),
            )
        }, mockk<QalipsisMeterSnapshot<DistributionSummary>> {
            every { timestamp } returns now
            every { meter } returns summaryMock
            every { measurements } returns listOf(
                MeasurementMetric(22.0, Statistic.MEAN),
                MeasurementMetric(17873213.0, Statistic.TOTAL),
                MeasurementMetric(548.5, Statistic.MAX),
                DistributionMeasurementMetric(548.5, Statistic.PERCENTILE, 85.0),
                DistributionMeasurementMetric(54328.5, Statistic.PERCENTILE, 50.0),
            )
        })

        // when
        measurementPublisher.publish(meterSnapshots)
        val published = mutableListOf<MeterSnapshot>()
        consumer.subscribe(listOf(configuration.topic))
        do {
            val consumed = consumer.poll(Duration.ofSeconds(10)).map(ConsumerRecord<ByteArray, MeterSnapshot>::value)
            published += consumed
        } while (published.size < 4)

        // then
        // We keep only the values of the first step, because the timer is only affected by our operation
        // in that period of time.
        val firstPublishedValues = published.sortedBy { it.timestamp }.distinctBy { it::class }
        val timerSnapshot: TimerSnapshot = firstPublishedValues.filterIsInstance<TimerSnapshot>().first()
        val gaugeSnapshot: GaugeSnapshot = firstPublishedValues.filterIsInstance<GaugeSnapshot>().first()
        val counterSnapshot: CounterSnapshot = firstPublishedValues.filterIsInstance<CounterSnapshot>().first()
        val summarySnapshot: DistributionSummarySnapshot = firstPublishedValues.filterIsInstance<DistributionSummarySnapshot>().first()
        assertThat(timerSnapshot).all {
            prop(TimerSnapshot::timestamp).isNotNull().isEqualTo(now)
            prop(TimerSnapshot::sum).isEqualTo(40000.0)
            prop(TimerSnapshot::max).isEqualTo(2000.0)
            prop(TimerSnapshot::name).isEqualTo("the-timer")
            prop(TimerSnapshot::mean).isEqualTo(2.0)
            prop(TimerSnapshot::others).all {
                hasSize(3)
                key("tag-2").isEqualTo("value-2")
                key("percentile_50.0").isEqualTo(5432844.5)
                key("percentile_85.0").isEqualTo(5.000004485E8)
            }
        }
        assertThat(summarySnapshot).all {
            prop(DistributionSummarySnapshot::timestamp).isNotNull().isEqualTo(now)
            prop(DistributionSummarySnapshot::sum).isEqualTo(1.7873213E7)
            prop(DistributionSummarySnapshot::max).isEqualTo(548.5)
            prop(DistributionSummarySnapshot::name).isEqualTo("the-summary")
            prop(DistributionSummarySnapshot::mean).isEqualTo(22.0)
            prop(DistributionSummarySnapshot::others).all {
                hasSize(3)
                key("tag-4").isEqualTo("value-4")
                key("percentile_85.0").isEqualTo(548.5)
                key("percentile_50.0").isEqualTo(54328.5)
            }
        }
        assertThat(gaugeSnapshot).all {
            prop(GaugeSnapshot::timestamp).isNotNull().isEqualTo(now)
            prop(GaugeSnapshot::value).isEqualTo(654.0)
            prop(GaugeSnapshot::name).isEqualTo("the-gauge")
            prop(GaugeSnapshot::others).all {
                hasSize(1)
                key("tag-3").isEqualTo("value-3")
            }
        }
        assertThat(counterSnapshot).all {
            prop(CounterSnapshot::timestamp).isNotNull().isEqualTo(now)
            prop(CounterSnapshot::count).isEqualTo(8)
            prop(CounterSnapshot::name).isEqualTo("the-counter")
            prop(CounterSnapshot::others).all {
                hasSize(1)
                key("tag-1").isEqualTo("value-1")
            }
        }
    }

    class JacksonMeterDeserializer() : Deserializer<MeterSnapshot> {

        private val objectMapper = jacksonObjectMapper().apply {
            registerModule(JavaTimeModule())
        }

        override fun deserialize(topic: String?, data: ByteArray): MeterSnapshot {
            return objectMapper.readValue(data)
        }
    }

    private fun getTimeMock(): Instant {
        val now = Instant.now()
        val fixedClock = Clock.fixed(now, ZoneId.systemDefault())
        mockkStatic(Clock::class)
        every { Clock.systemUTC() } returns fixedClock

        return now
    }

    companion object {
        @Container
        @JvmStatic
        private val CONTAINER = KafkaContainer(DockerImageName.parse(Constants.DOCKER_IMAGE)).apply {
            withCreateContainerCmdModifier { cmd ->
                cmd.hostConfig!!.withMemory(512 * 1024.0.pow(2).toLong()).withCpuCount(2)
            }
            withEnv("KAFKA_HEAP_OPTS", "-Xms256m -Xmx256m")
        }
    }
}
