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
import assertk.assertions.any
import assertk.assertions.hasSize
import assertk.assertions.isEqualTo
import assertk.assertions.isInstanceOf
import assertk.assertions.isNotNull
import assertk.assertions.key
import assertk.assertions.prop
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.PropertySource
import io.micronaut.core.util.StringUtils
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import io.mockk.every
import io.mockk.mockk
import io.qalipsis.api.meters.DistributionMeasurementMetric
import io.qalipsis.api.meters.MeasurementMetric
import io.qalipsis.api.meters.Meter
import io.qalipsis.api.meters.MeterType
import io.qalipsis.api.meters.Statistic
import io.qalipsis.plugins.kafka.Constants
import io.qalipsis.plugins.kafka.config.KafkaMeasurementPublisherFactory
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.qalipsis.test.mockk.WithMockk
import jakarta.inject.Inject
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
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.extension.RegisterExtension
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.time.Instant
import java.util.Properties
import java.util.concurrent.TimeUnit
import kotlin.math.pow

@WithMockk
@Testcontainers
@MicronautTest(startApplication = false)
@PropertySource(
    Property(name = "meters.export.enabled", value = StringUtils.TRUE),
    Property(name = "meters.export.kafka.enabled", value = StringUtils.TRUE)
)
internal class KafkaMeasurementPublisherIntegrationTest : TestPropertyProvider {

    @JvmField
    @RegisterExtension
    val testDispatcherProvider = TestDispatcherProvider()

    private lateinit var bootstrapServers: String

    private lateinit var consumer: KafkaConsumer<ByteArray, MeterSnapshot>

    @Inject
    private lateinit var measurementPublisherFactory: KafkaMeasurementPublisherFactory

    override fun getProperties(): Map<String, String> {
        bootstrapServers = CONTAINER.bootstrapServers.substringAfter("PLAINTEXT://")
        return mapOf(
            "${KafkaMeterConfig.KAFKA_CONFIGURATION}.bootstrap" to bootstrapServers,
            "${KafkaMeterConfig.KAFKA_CONFIGURATION}.topic" to "the-topic-for-measurements"
        )
    }

    @BeforeAll
    internal fun setUpAll() {
        val consumerProperties = Properties()
        consumerProperties[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        consumerProperties[ConsumerConfig.GROUP_ID_CONFIG] = "test"
        consumerProperties[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "${OffsetResetStrategy.EARLIEST}".lowercase()
        consumerProperties[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = false
        consumer = KafkaConsumer(consumerProperties, Serdes.ByteArray().deserializer(), JacksonMeterDeserializer())
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
        val now = Instant.now()
        val meterSnapshots = listOf(
            mockk<io.qalipsis.api.meters.MeterSnapshot> {
                every { timestamp } returns now
                every { meterId } returns Meter.Id(
                    "my counter",
                    MeterType.COUNTER,
                    mapOf(
                        "scenario" to "first scenario",
                        "campaign" to "first campaign 5473653",
                        "step" to "step number one"
                    )
                )
                every { measurements } returns listOf(MeasurementMetric(8.0, Statistic.COUNT))
            },
            mockk<io.qalipsis.api.meters.MeterSnapshot> {
                every { timestamp } returns now
                every { meterId } returns Meter.Id(
                    "my gauge",
                    MeterType.GAUGE,
                    mapOf(
                        "scenario" to "third scenario",
                        "campaign" to "third CAMPAIGN 7624839",
                        "step" to "step number three",
                        "foo" to "bar",
                        "any-tag" to "any-value"
                    )
                )
                every { measurements } returns listOf(MeasurementMetric(5.0, Statistic.VALUE))
            }, mockk<io.qalipsis.api.meters.MeterSnapshot> {
                every { timestamp } returns now
                every { meterId } returns Meter.Id(
                    "my timer",
                    MeterType.TIMER,
                    mapOf(
                        "scenario" to "second scenario",
                        "campaign" to "second campaign 47628233",
                        "step" to "step number two",
                    )
                )
                every { measurements } returns listOf(
                    MeasurementMetric(80.0, Statistic.COUNT),
                    MeasurementMetric(224.0, Statistic.MEAN),
                    MeasurementMetric(178713.0, Statistic.TOTAL_TIME),
                    MeasurementMetric(54328.5, Statistic.MAX),
                    DistributionMeasurementMetric(500000448.5, Statistic.PERCENTILE, 85.0),
                    DistributionMeasurementMetric(5432844.5, Statistic.PERCENTILE, 50.0),
                )
            }, mockk<io.qalipsis.api.meters.MeterSnapshot> {
                every { timestamp } returns now
                every { meterId } returns Meter.Id(
                    "my final summary",
                    MeterType.DISTRIBUTION_SUMMARY,
                    mapOf(
                        "scenario" to "fourth scenario",
                        "campaign" to "fourth CAMPAIGN 283239",
                        "step" to "step number four",
                        "dist" to "summary",
                        "local" to "host"
                    )
                )
                every { measurements } returns listOf(
                    MeasurementMetric(70.0, Statistic.COUNT),
                    MeasurementMetric(22.0, Statistic.MEAN),
                    MeasurementMetric(17873213.0, Statistic.TOTAL),
                    MeasurementMetric(548.5, Statistic.MAX),
                    DistributionMeasurementMetric(548.5, Statistic.PERCENTILE, 85.0),
                    DistributionMeasurementMetric(54328.5, Statistic.PERCENTILE, 50.0),
                )
            },
            mockk<io.qalipsis.api.meters.MeterSnapshot> {
                every { timestamp } returns now
                every { meterId } returns Meter.Id(
                    "my Rate",
                    MeterType.RATE,
                    mapOf(
                        "scenario" to "fifth scenario",
                        "campaign" to "campaign 39",
                        "step" to "step number five",
                        "foo" to "bar",
                        "local" to "host"
                    )
                )
                every { measurements } returns listOf(
                    MeasurementMetric(2.0, Statistic.VALUE)
                )
            },
            mockk<io.qalipsis.api.meters.MeterSnapshot> {
                every { timestamp } returns now
                every { meterId } returns Meter.Id(
                    "throughput",
                    MeterType.THROUGHPUT,
                    mapOf(
                        "scenario" to "sixth scenario",
                        "campaign" to "CEAD@E28339",
                        "step" to "step number six",
                        "a" to "b",
                        "c" to "d"
                    )
                )
                every { measurements } returns listOf(
                    MeasurementMetric(30.0, Statistic.VALUE),
                    MeasurementMetric(22.0, Statistic.MEAN),
                    MeasurementMetric(173.0, Statistic.TOTAL),
                    MeasurementMetric(42.0, Statistic.MAX),
                    DistributionMeasurementMetric(42.0, Statistic.PERCENTILE, 85.0),
                    DistributionMeasurementMetric(30.0, Statistic.PERCENTILE, 50.0),
                )
            })

        // when
        measurementPublisher.publish(meterSnapshots)
        val publishedValues = mutableListOf<MeterSnapshot>()
        consumer.subscribe(listOf("the-topic-for-measurements"))
        do {
            val consumed = consumer.poll(Duration.ofSeconds(10)).map(ConsumerRecord<ByteArray, MeterSnapshot>::value)
            publishedValues += consumed
        } while (publishedValues.size < 6)

        // then
        assertThat(publishedValues).all {
            hasSize(6)
            any {
                it.isInstanceOf<TimerSnapshot>().all {
                    prop(TimerSnapshot::name).isEqualTo("my timer")
                    prop(TimerSnapshot::timestamp).isNotNull().isEqualTo(now)
                    prop(TimerSnapshot::sum).isEqualTo(178713.0)
                    prop(TimerSnapshot::max).isEqualTo(54328.5)
                    prop(TimerSnapshot::mean).isEqualTo(224.0)
                    prop(TimerSnapshot::count).isEqualTo(80)
                    prop(TimerSnapshot::unit).isEqualTo(TimeUnit.MICROSECONDS)
                    prop(TimerSnapshot::tags).isNotNull().all {
                        hasSize(3)
                        key("campaign").isEqualTo("second campaign 47628233")
                        key("scenario").isEqualTo("second scenario")
                        key("step").isEqualTo("step number two")
                    }
                    prop(TimerSnapshot::others).all {
                        hasSize(2)
                        key("percentile_50_0").isEqualTo(5432844.5)
                        key("percentile_85_0").isEqualTo(5.000004485E8)
                    }
                }
            }
            any {
                it.isInstanceOf<DistributionSummarySnapshot>().all {
                    prop(DistributionSummarySnapshot::name).isEqualTo("my final summary")
                    prop(DistributionSummarySnapshot::timestamp).isNotNull().isEqualTo(now)
                    prop(DistributionSummarySnapshot::sum).isEqualTo(17873213.0)
                    prop(DistributionSummarySnapshot::max).isEqualTo(548.5)
                    prop(DistributionSummarySnapshot::mean).isEqualTo(22.0)
                    prop(DistributionSummarySnapshot::count).isEqualTo(70)
                    prop(DistributionSummarySnapshot::tags).isNotNull().all {
                        hasSize(5)
                        key("campaign").isEqualTo("fourth CAMPAIGN 283239")
                        key("scenario").isEqualTo("fourth scenario")
                        key("step").isEqualTo("step number four")
                        key("dist").isEqualTo("summary")
                        key("local").isEqualTo("host")
                    }
                    prop(DistributionSummarySnapshot::others).all {
                        hasSize(2)
                        key("percentile_50_0").isEqualTo(54328.5)
                        key("percentile_85_0").isEqualTo(548.5)
                    }
                }
            }
            any {
                it.isInstanceOf<CounterSnapshot>().all {
                    prop(CounterSnapshot::name).isEqualTo("my counter")
                    prop(CounterSnapshot::timestamp).isNotNull().isEqualTo(now)
                    prop(CounterSnapshot::count).isEqualTo(8)
                    prop(CounterSnapshot::tags).isNotNull().all {
                        hasSize(3)
                        key("campaign").isEqualTo("first campaign 5473653")
                        key("scenario").isEqualTo("first scenario")
                        key("step").isEqualTo("step number one")
                    }
                }
            }
            any {
                it.isInstanceOf<GaugeSnapshot>().all {
                    prop(GaugeSnapshot::name).isEqualTo("my gauge")
                    prop(GaugeSnapshot::timestamp).isNotNull().isEqualTo(now)
                    prop(GaugeSnapshot::value).isEqualTo(5.0)
                    prop(GaugeSnapshot::tags).isNotNull().all {
                        hasSize(5)
                        key("campaign").isEqualTo("third CAMPAIGN 7624839")
                        key("scenario").isEqualTo("third scenario")
                        key("step").isEqualTo("step number three")
                        key("foo").isEqualTo("bar")
                        key("any-tag").isEqualTo("any-value")
                    }
                }
            }
            any {
                it.isInstanceOf<RateSnapshot>().all {
                    prop(RateSnapshot::name).isEqualTo("my Rate")
                    prop(RateSnapshot::timestamp).isNotNull().isEqualTo(now)
                    prop(RateSnapshot::value).isEqualTo(2.0)
                    prop(RateSnapshot::tags).isNotNull().all {
                        hasSize(5)
                        key("campaign").isEqualTo("campaign 39")
                        key("scenario").isEqualTo("fifth scenario")
                        key("step").isEqualTo("step number five")
                        key("foo").isEqualTo("bar")
                        key("local").isEqualTo("host")
                    }
                }
            }
            any {
                it.isInstanceOf<ThroughputSnapshot>().all {
                    prop(ThroughputSnapshot::name).isEqualTo("throughput")
                    prop(ThroughputSnapshot::timestamp).isNotNull().isEqualTo(now)
                    prop(ThroughputSnapshot::sum).isEqualTo(173.0)
                    prop(ThroughputSnapshot::max).isEqualTo(42.0)
                    prop(ThroughputSnapshot::mean).isEqualTo(22.0)
                    prop(ThroughputSnapshot::value).isEqualTo(30.0)
                    prop(ThroughputSnapshot::tags).isNotNull().all {
                        hasSize(5)
                        key("campaign").isEqualTo("CEAD@E28339")
                        key("scenario").isEqualTo("sixth scenario")
                        key("step").isEqualTo("step number six")
                        key("a").isEqualTo("b")
                        key("c").isEqualTo("d")
                    }
                    prop(ThroughputSnapshot::others).all {
                        hasSize(2)
                        key("percentile_50_0").isEqualTo(30.0)
                        key("percentile_85_0").isEqualTo(42.0)
                    }
                }
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
