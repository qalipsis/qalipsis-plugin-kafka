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
import io.qalipsis.test.mockk.relaxedMockk
import io.qalipsis.test.mockk.verifyOnce
import io.qalipsis.test.steps.AbstractStepSpecificationConverterTest
import kotlinx.coroutines.test.runBlockingTest
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.serialization.Serdes
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
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

    @Test
    override fun `should not support unexpected spec`() {
        Assertions.assertFalse(converter.support(relaxedMockk()))
    }

    @Test
    override fun `should support expected spec`() {
        Assertions.assertTrue(converter.support(relaxedMockk<KafkaConsumerStepSpecification<*, *>>()))
    }

    @Test
    internal fun `should convert spec with name and list of topics and generate a batch output`() = runBlockingTest {
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
                prop("id").isEqualTo("my-step")
                prop("reader").isNotNull().isInstanceOf(KafkaConsumerIterativeReader::class).all {
                    prop("stepId").isEqualTo("my-step")
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
    internal fun `should convert spec without name but a topic pattern and generate a flat output`() = runBlockingTest {
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
                prop("id").isNotNull()
                prop("reader").isNotNull().isInstanceOf(KafkaConsumerIterativeReader::class).all {
                    prop("stepId").isEqualTo(step.id)
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