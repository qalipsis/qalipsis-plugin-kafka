/*
 * QALIPSIS
 * Copyright (C) 2025 AERIS IT Solutions GmbH
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package io.qalipsis.plugins.kafka.consumer

import assertk.all
import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.index
import assertk.assertions.isEqualTo
import assertk.assertions.isNotNull
import assertk.assertions.isSameAs
import assertk.assertions.key
import assertk.assertions.prop
import io.mockk.coEvery
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.mockk.verifyOrder
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.meters.CampaignMeterRegistry
import io.qalipsis.api.meters.Counter
import io.qalipsis.api.meters.Meter
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.qalipsis.test.mockk.CleanMockkRecordedCalls
import io.qalipsis.test.mockk.relaxedMockk
import kotlinx.coroutines.channels.Channel
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.serialization.Deserializer
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.extension.RegisterExtension
import java.util.concurrent.atomic.AtomicLong

/**
 *
 * @author Eric Jessé
 */
@CleanMockkRecordedCalls
internal class KafkaConsumerBatchConverterTest {

    @JvmField
    @RegisterExtension
    val testDispatcherProvider = TestDispatcherProvider()

    private val keySerializer: Deserializer<Int> = relaxedMockk {
        every { deserialize(any(), any(), any()) } answers { thirdArg<ByteArray?>()?.size ?: Int.MIN_VALUE }
    }

    private val valueSerializer: Deserializer<Int> = relaxedMockk {
        every { deserialize(any(), any(), any()) } answers { thirdArg<ByteArray?>()?.size ?: Int.MAX_VALUE }
    }

    private val metersTags = mockk<Map<String, String>>()

    private val eventsTags = mockk<Map<String, String>>()

    private val startStopContext = relaxedMockk<StepStartStopContext> {
        every { toMetersTags() } returns metersTags
        every { toEventTags() } returns eventsTags
        every { scenarioName } returns "scenario-name"
        every { stepName } returns "step-name"
    }

    private val consumedKeyBytesCounter = relaxedMockk<Counter>()

    private val consumedValueBytesCounter = relaxedMockk<Counter>()

    private val consumedRecordsCounter = relaxedMockk<Counter>()

    private val eventsLogger = relaxedMockk<EventsLogger>()

    private val meterRegistry = relaxedMockk<CampaignMeterRegistry> {
        every {
            counter(
                "scenario-name",
                "step-name",
                "kafka-consume-key-bytes",
                refEq(metersTags)
            )
        } returns consumedKeyBytesCounter
        every { consumedKeyBytesCounter.report(any()) } returns consumedKeyBytesCounter
        every {
            counter(
                "scenario-name",
                "step-name",
                "kafka-consume-value-bytes",
                refEq(metersTags)
            )
        } returns consumedValueBytesCounter
        every { consumedValueBytesCounter.report(any()) } returns consumedValueBytesCounter
        every {
            counter(
                "scenario-name",
                "step-name",
                "kafka-consume-records",
                refEq(metersTags)
            )
        } returns consumedRecordsCounter
        every { consumedRecordsCounter.report(any()) } returns consumedRecordsCounter
    }

    @Test
    @Timeout(2)
    internal fun `should deserialize without monitoring`() {
        // when
        executeConversion()

        confirmVerified(
            consumedKeyBytesCounter,
            consumedValueBytesCounter,
            consumedRecordsCounter,
            keySerializer,
            valueSerializer
        )
    }

    @Test
    @Timeout(2)
    internal fun `should deserialize and monitor`() {
        // when
        executeConversion(meterRegistry, eventsLogger)

        verifyOrder {
            consumedKeyBytesCounter.report(any<Meter.ReportingConfiguration<Counter>.() -> Unit>())
            consumedValueBytesCounter.report(any<Meter.ReportingConfiguration<Counter>.() -> Unit>())
            consumedRecordsCounter.report(any<Meter.ReportingConfiguration<Counter>.() -> Unit>())
            consumedRecordsCounter.increment(3.0)
            consumedKeyBytesCounter.increment(22.0)
            consumedValueBytesCounter.increment(44.0)

            eventsLogger.info("kafka.consume.consumed.records", 3, any(), tags = refEq(eventsTags))
            eventsLogger.info("kafka.consume.consumed.key-bytes", 22, any(), tags = refEq(eventsTags))
            eventsLogger.info("kafka.consume.consumed.value-bytes", 44, any(), tags = refEq(eventsTags))
        }
        confirmVerified(
            consumedValueBytesCounter,
            consumedKeyBytesCounter,
            consumedRecordsCounter,
            keySerializer,
            valueSerializer,
            eventsLogger
        )
    }

    private fun executeConversion(
        meterRegistry: CampaignMeterRegistry? = null,
        eventsLogger: EventsLogger? = null
    ) = testDispatcherProvider.runTest {
        // given
        val key1 = ByteArray(10)
        val value1 = ByteArray(20)
        val headers1 = RecordHeaders()
        val key2 = ByteArray(12)
        val value2 = ByteArray(24)
        val headers2 = RecordHeaders(listOf(RecordHeader("header2", value2)))
        val key3: ByteArray? = null
        val value3: ByteArray? = null
        val headers3 = RecordHeaders()
        val converter = KafkaConsumerBatchConverter(
            keySerializer,
            valueSerializer, meterRegistry, eventsLogger
        )
        val consumedOffset = AtomicLong(123)
        val channel = Channel<KafkaConsumerResults<Int, Int>>(1)

        converter.start(startStopContext)
        //when
        converter.supply(
            consumedOffset, ConsumerRecords(
                mapOf(
                    TopicPartition("topic-1", 0) to listOf(
                        record("topic-1", 11, key1, value1, headers1)
                    ),
                    TopicPartition("topic-2", 0) to listOf(
                        record("topic-2", 22, key2, value2, headers2),
                        record("topic-2", 33, key3, value3, headers3)
                    )
                )
            ), relaxedMockk { coEvery { send(any()) } coAnswers { channel.send(firstArg()) } }
        )
        val results = channel.receive()

        // then
        assertThat(results).all {
            prop(KafkaConsumerResults<*, *>::records).all {
                index(0).all {
                    prop(KafkaConsumerRecord<*, *>::key).isEqualTo(key1.size)
                    prop(KafkaConsumerRecord<*, *>::value).isEqualTo(value1.size)
                    prop(KafkaConsumerRecord<*, *>::headers).hasSize(0)
                    prop(KafkaConsumerRecord<*, *>::consumedTimestamp).isNotNull()
                    prop(KafkaConsumerRecord<*, *>::offset).isEqualTo(11)
                    prop(KafkaConsumerRecord<*, *>::topic).isEqualTo("topic-1")
                    prop(KafkaConsumerRecord<*, *>::partition).isEqualTo(5)
                    prop(KafkaConsumerRecord<*, *>::receivedTimestamp).isEqualTo(12)
                }

                index(1).all {
                    prop(KafkaConsumerRecord<*, *>::key).isEqualTo(key2.size)
                    prop(KafkaConsumerRecord<*, *>::value).isEqualTo(value2.size)
                    prop(KafkaConsumerRecord<*, *>::headers).all {
                        hasSize(1)
                        key("header2").isSameAs(value2)
                    }
                    prop(KafkaConsumerRecord<*, *>::consumedTimestamp).isNotNull()
                    prop(KafkaConsumerRecord<*, *>::offset).isEqualTo(22)
                    prop(KafkaConsumerRecord<*, *>::topic).isEqualTo("topic-2")
                    prop(KafkaConsumerRecord<*, *>::partition).isEqualTo(5)
                    prop(KafkaConsumerRecord<*, *>::receivedTimestamp).isEqualTo(23)
                }

                index(2).all {
                    prop(KafkaConsumerRecord<*, *>::key).isEqualTo(Int.MIN_VALUE)
                    prop(KafkaConsumerRecord<*, *>::value).isEqualTo(Int.MAX_VALUE)
                    prop(KafkaConsumerRecord<*, *>::headers).hasSize(0)
                    prop(KafkaConsumerRecord<*, *>::consumedTimestamp).isNotNull()
                    prop(KafkaConsumerRecord<*, *>::offset).isEqualTo(33)
                    prop(KafkaConsumerRecord<*, *>::topic).isEqualTo("topic-2")
                    prop(KafkaConsumerRecord<*, *>::partition).isEqualTo(5)
                    prop(KafkaConsumerRecord<*, *>::receivedTimestamp).isEqualTo(34)
                }
            }
            prop(KafkaConsumerResults<*, *>::meters).all {
                prop(KafkaConsumerMeters::recordsCount).isEqualTo(3)
                prop(KafkaConsumerMeters::keysBytesReceived).isEqualTo(22)
                prop(KafkaConsumerMeters::valuesBytesReceived).isEqualTo(44)
            }
        }

        verify {
            keySerializer.deserialize("topic-1", refEq(headers1), refEq(key1))
            valueSerializer.deserialize("topic-1", refEq(headers1), refEq(value1))
            keySerializer.deserialize("topic-2", refEq(headers2), refEq(key2))
            valueSerializer.deserialize("topic-2", refEq(headers2), refEq(value2))
            keySerializer.deserialize("topic-2", refEq(headers3), isNull())
            valueSerializer.deserialize("topic-2", refEq(headers3), isNull())
        }
    }

    private fun record(
        topic: String, offset: Long, key: ByteArray?, value: ByteArray?,
        headers: Headers
    ): ConsumerRecord<ByteArray?, ByteArray?> {
        return ConsumerRecord(
            topic, 5, offset, offset + 1, TimestampType.CREATE_TIME, 123L, key?.size ?: -1,
            value?.size ?: -1, key, value, headers
        )
    }
}