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
import assertk.assertions.containsOnly
import assertk.assertions.hasSize
import assertk.assertions.isEqualTo
import assertk.assertions.isInstanceOf
import assertk.assertions.isNotNull
import assertk.assertions.isNull
import assertk.assertions.isSameAs
import assertk.assertions.key
import io.aerisconsulting.catadioptre.getProperty
import io.aerisconsulting.catadioptre.invokeInvisible
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.spyk
import io.qalipsis.api.steps.StepCreationContext
import io.qalipsis.api.steps.StepCreationContextImpl
import io.qalipsis.api.steps.StepMonitoringConfiguration
import io.qalipsis.api.steps.datasource.DatasourceObjectConverter
import io.qalipsis.api.steps.datasource.IterativeDatasourceStep
import io.qalipsis.api.steps.datasource.processors.NoopDatasourceObjectProcessor
import io.qalipsis.test.assertk.prop
import io.qalipsis.test.assertk.typedProp
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.qalipsis.test.mockk.relaxedMockk
import io.qalipsis.test.mockk.verifyOnce
import io.qalipsis.test.steps.AbstractStepSpecificationConverterTest
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.serialization.Serdes
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import java.time.Duration
import java.util.Properties
import java.util.regex.Pattern

/**
 *
 * @author Eric Jessé
 */
@Suppress("UNCHECKED_CAST")
internal class KafkaConsumerStepSpecificationConverterTest :
    AbstractStepSpecificationConverterTest<KafkaConsumerStepSpecificationConverter>() {

    @JvmField
    @RegisterExtension
    val testDispatcherProvider = TestDispatcherProvider()

    @Test
    override fun `should not support unexpected spec`() {
        Assertions.assertFalse(converter.support(relaxedMockk()))
    }

    @Test
    override fun `should support expected spec`() {
        Assertions.assertTrue(converter.support(relaxedMockk<KafkaConsumerStepSpecification<*, *>>()))
    }

    @Test
    internal fun `should convert spec with name and list of topics and generate a batch output`() =
        testDispatcherProvider.runTest {
            // given
            val keyDeserializer = Serdes.ByteArray().deserializer()
            val valueDeserializer = Serdes.ByteArray().deserializer()
            val spec = KafkaConsumerStepSpecification(keyDeserializer, valueDeserializer)
            spec.apply {
                name = "my-step"
                bootstrap("my-bootstrap")
                concurrency(2)
                topics("topic-1", "topic-2")
                pollTimeout(3)
            maxPolledRecords(4)
            groupId("my-group")
            offsetReset(OffsetResetStrategy.EARLIEST)
            properties("key-1" to "value-1")
            properties("key-2" to "value-2", "key-3" to 5)
        }
        val creationContext = StepCreationContextImpl(scenarioSpecification, directedAcyclicGraph, spec)
        val spiedConverter = spyk(converter, recordPrivateCalls = true)
        val recordsConverter: DatasourceObjectConverter<ConsumerRecords<ByteArray?, ByteArray?>, out Any?> =
            relaxedMockk()
        every {
            spiedConverter["buildConverter"](
                refEq(spec.configuration),
                refEq(spec.monitoringConfig)
            )
        } returns recordsConverter

        // when
        spiedConverter.convert<Unit, Map<String, *>>(
            creationContext as StepCreationContext<KafkaConsumerStepSpecification<*, *>>
        )

        // then
        creationContext.createdStep!!.let {
            assertThat(it).isInstanceOf(IterativeDatasourceStep::class).all {
                prop("name").isEqualTo("my-step")
                prop("reader").isNotNull().isInstanceOf(KafkaConsumerIterativeReader::class).all {
                    prop("stepName").isEqualTo("my-step")
                    typedProp<Properties>("props").all {
                        hasSize(7)
                        key(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG).isEqualTo("my-bootstrap")
                        key(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG).isEqualTo("earliest")
                        key(ConsumerConfig.MAX_POLL_RECORDS_CONFIG).isEqualTo(4)
                        key(ConsumerConfig.GROUP_ID_CONFIG).isEqualTo("my-group")
                        key("key-1").isEqualTo("value-1")
                        key("key-2").isEqualTo("value-2")
                        key("key-3").isEqualTo(5)
                    }
                    prop("pollTimeout").isEqualTo(Duration.ofMillis(3))
                    prop("concurrency").isEqualTo(2)
                    typedProp<Collection<String>>("topics").containsOnly("topic-1", "topic-2")
                    typedProp<Pattern>("topicsPattern").isNull()
                }
                prop("processor").isNotNull().isInstanceOf(NoopDatasourceObjectProcessor::class)
                prop("converter").isNotNull().isSameAs(recordsConverter)
            }
        }
    }

    @Test
    internal fun `should convert spec without name but a topic pattern and generate a flat output`() =
        testDispatcherProvider.runTest {
            // given
            val keyDeserializer = Serdes.ByteArray().deserializer()
            val valueDeserializer = Serdes.ByteArray().deserializer()
            val spec = KafkaConsumerStepSpecification(keyDeserializer, valueDeserializer)
            spec.apply {
                topicsPattern(Pattern.compile(".*"))
                offsetReset(OffsetResetStrategy.NONE)
                (getProperty("configuration") as KafkaConsumerConfiguration<*, *>).flattenOutput = true
            }
            val creationContext = StepCreationContextImpl(scenarioSpecification, directedAcyclicGraph, spec)
        val spiedConverter = spyk(converter, recordPrivateCalls = true)
        val recordsConverter: DatasourceObjectConverter<ConsumerRecords<ByteArray?, ByteArray?>, out Any?> =
            relaxedMockk()
        every {
            spiedConverter["buildConverter"](
                refEq(spec.configuration),
                refEq(spec.monitoringConfig)
            )
        } returns recordsConverter

        // when
        spiedConverter.convert<Unit, Map<String, *>>(
            creationContext as StepCreationContext<KafkaConsumerStepSpecification<*, *>>
        )

        // then
        verifyOnce {
            spiedConverter["buildConverter"](
                refEq(spec.configuration),
                refEq(spec.monitoringConfig)
            )
        }
        creationContext.createdStep!!.let { step ->
            assertThat(step).isInstanceOf(IterativeDatasourceStep::class).all {
                prop("name").isNotNull()
                prop("reader").isNotNull().isInstanceOf(KafkaConsumerIterativeReader::class).all {
                    prop("stepName").isEqualTo(step.name)
                    typedProp<Properties>("props").all {
                        hasSize(4)
                        key(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG).isEqualTo("localhost:9092")
                        key(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG).isEqualTo("none")
                        key(ConsumerConfig.MAX_POLL_RECORDS_CONFIG).isEqualTo(500)
                        key(ConsumerConfig.GROUP_ID_CONFIG).isEqualTo("")
                    }
                    prop("pollTimeout").isEqualTo(Duration.ofSeconds(1))
                    prop("concurrency").isEqualTo(1)
                    typedProp<Collection<String>>("topics").hasSize(0)
                    typedProp<Pattern>("topicsPattern").transform { it.pattern() }.isEqualTo(".*")
                }
                prop("processor").isNotNull().isInstanceOf(NoopDatasourceObjectProcessor::class)
                prop("converter").isNotNull().isSameAs(recordsConverter)
            }
        }
    }

    @Test
    internal fun `should build single converter`() {
        // given
        val keyDeserializer = Serdes.ByteArray().deserializer()
        val valueDeserializer = Serdes.ByteArray().deserializer()
        val configuration = KafkaConsumerConfiguration(
            keyDeserializer = keyDeserializer,
            valueDeserializer = valueDeserializer,
            flattenOutput = true
        )
        val monitoringConfiguration = StepMonitoringConfiguration()

        // when
        val recordsConverter = converter.invokeInvisible<DatasourceObjectConverter<ConsumerRecords<ByteArray?, ByteArray?>, out Any?>>("buildConverter", configuration, monitoringConfiguration)

        // then
        assertThat(recordsConverter).isNotNull().isInstanceOf(KafkaConsumerSingleConverter::class).all {
            prop("keyDeserializer").isSameAs(keyDeserializer)
            prop("valueDeserializer").isSameAs(valueDeserializer)
            prop("consumedKeyBytesCounter").isNull()
            prop("consumedValueBytesCounter").isNull()
            prop("consumedRecordsCounter").isNull()
        }
    }

    @Test
    internal fun `should build batch converter`() {
        // given
        val keyDeserializer = Serdes.ByteArray().deserializer()
        val valueDeserializer = Serdes.ByteArray().deserializer()
        val configuration = KafkaConsumerConfiguration(
            keyDeserializer = keyDeserializer,
            valueDeserializer = valueDeserializer,
            flattenOutput = false
        )
        val monitoringConfiguration = StepMonitoringConfiguration()

        // when
        val recordsConverter = converter.invokeInvisible<DatasourceObjectConverter<ConsumerRecords<ByteArray?, ByteArray?>, out Any?>>("buildConverter", configuration, monitoringConfiguration)

        // then
        assertThat(recordsConverter).isNotNull().isInstanceOf(KafkaConsumerBatchConverter::class).all {
            prop("keyDeserializer").isSameAs(keyDeserializer)
            prop("valueDeserializer").isSameAs(valueDeserializer)
            prop("consumedKeyBytesCounter").isNull()
            prop("consumedValueBytesCounter").isNull()
            prop("consumedRecordsCounter").isNull()
        }
        confirmVerified(meterRegistry)
    }
}