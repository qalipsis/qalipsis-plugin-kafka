package io.qalipsis.plugins.kafka.consumer

import assertk.all
import assertk.assertThat
import assertk.assertions.*
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import io.mockk.coEvery
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.verify
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.events.EventsLogger
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
internal class KafkaConsumerSingleConverterTest {

    @JvmField
    @RegisterExtension
    val testDispatcherProvider = TestDispatcherProvider()

    private val keySerializer: Deserializer<Int> = relaxedMockk {
        every { deserialize(any(), any(), any()) } answers { thirdArg<ByteArray?>()?.size ?: Int.MIN_VALUE }
    }

    private val valueSerializer: Deserializer<Int> = relaxedMockk {
        every { deserialize(any(), any(), any()) } answers { thirdArg<ByteArray?>()?.size ?: Int.MAX_VALUE }
    }

    private val counter: Counter = relaxedMockk {}

    private val metersTags = relaxedMockk<Tags>()

    private val startStopContext = relaxedMockk<StepStartStopContext> {
        every { toMetersTags() } returns metersTags
    }

    private val consumedKeyBytesCounter = relaxedMockk<Counter>()

    private val consumedValueBytesCounter = relaxedMockk<Counter>()

    private val consumedRecordsCounter = relaxedMockk<Counter>()

    private val eventsLogger = relaxedMockk<EventsLogger>()

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
    internal fun `should deserialize with monitoring`() {
        val tags: Map<String, String> = emptyMap()

        val meterRegistry = relaxedMockk<MeterRegistry> {
            every { counter("kafka-consume-key-bytes", refEq(metersTags)) } returns consumedKeyBytesCounter
            every { counter("kafka-consume-value-bytes", refEq(metersTags)) } returns consumedValueBytesCounter
            every { counter("kafka-consume-records", refEq(metersTags)) } returns consumedRecordsCounter
        }
        // when
        executeConversion(meterRegistry, eventsLogger)

        verify {
            consumedRecordsCounter.increment(3.0)
            consumedKeyBytesCounter.increment(22.0)
            consumedValueBytesCounter.increment(44.0)

            eventsLogger.info("kafka.consume.consumed.records", 3, any(), tags = tags)
            eventsLogger.info("kafka.consume.consumed.key-bytes", 22, any(), tags = tags)
            eventsLogger.info("kafka.consume.consumed.value-bytes", 44, any(), tags = (tags))
        }

        confirmVerified(
            consumedValueBytesCounter,
            consumedKeyBytesCounter,
            consumedRecordsCounter,
            counter,
            keySerializer,
            valueSerializer,
            eventsLogger
        )
    }

    private fun executeConversion(
        meterRegistry: MeterRegistry? = null,
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
        val converter = KafkaConsumerSingleConverter(
            keySerializer, valueSerializer, meterRegistry, eventsLogger
        )
        val channel = Channel<KafkaConsumerResult<Int, Int>>(3)

        //when
        converter.start(startStopContext)
        converter.supply(
            AtomicLong(123), ConsumerRecords(
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
        // Each message is sent in a unitary statement.
        val results = listOf(channel.receive(), channel.receive(), channel.receive())

        // then
        assertThat(results).all {
            hasSize(3)
            index(0).all {
                prop(KafkaConsumerResult<*, *>::record).all {
                    prop(KafkaConsumerRecord<*, *>::key).isEqualTo(key1.size)
                    prop(KafkaConsumerRecord<*, *>::value).isEqualTo(value1.size)
                    prop(KafkaConsumerRecord<*, *>::headers).hasSize(0)
                    prop(KafkaConsumerRecord<*, *>::consumedTimestamp).isNotNull()
                    prop(KafkaConsumerRecord<*, *>::offset).isEqualTo(11)
                    prop(KafkaConsumerRecord<*, *>::topic).isEqualTo("topic-1")
                    prop(KafkaConsumerRecord<*, *>::partition).isEqualTo(5)
                    prop(KafkaConsumerRecord<*, *>::receivedTimestamp).isEqualTo(12)
                }
                prop(KafkaConsumerResult<*, *>::meters).all {
                    prop(KafkaConsumerMeters::recordsCount).isEqualTo(3)
                    prop(KafkaConsumerMeters::keysBytesReceived).isEqualTo(10)
                    prop(KafkaConsumerMeters::valuesBytesReceived).isEqualTo(20)
                }
            }

            index(1).all {
                prop(KafkaConsumerResult<*, *>::record).all {
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
                prop(KafkaConsumerResult<*, *>::meters).all {
                    prop(KafkaConsumerMeters::recordsCount).isEqualTo(3)
                    prop(KafkaConsumerMeters::keysBytesReceived).isEqualTo(12)
                    prop(KafkaConsumerMeters::valuesBytesReceived).isEqualTo(24)
                }
            }

            index(2).all {
                prop(KafkaConsumerResult<*, *>::record).all {
                    prop(KafkaConsumerRecord<*, *>::key).isEqualTo(Int.MIN_VALUE)
                    prop(KafkaConsumerRecord<*, *>::value).isEqualTo(Int.MAX_VALUE)
                    prop(KafkaConsumerRecord<*, *>::headers).hasSize(0)
                    prop(KafkaConsumerRecord<*, *>::consumedTimestamp).isNotNull()
                    prop(KafkaConsumerRecord<*, *>::offset).isEqualTo(33)
                    prop(KafkaConsumerRecord<*, *>::topic).isEqualTo("topic-2")
                    prop(KafkaConsumerRecord<*, *>::partition).isEqualTo(5)
                    prop(KafkaConsumerRecord<*, *>::receivedTimestamp).isEqualTo(34)
                }
                prop(KafkaConsumerResult<*, *>::meters).all {
                    prop(KafkaConsumerMeters::recordsCount).isEqualTo(3)
                    prop(KafkaConsumerMeters::keysBytesReceived).isEqualTo(-1)
                    prop(KafkaConsumerMeters::valuesBytesReceived).isEqualTo(-1)
                }
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