package io.qalipsis.plugins.kafka.producer

import assertk.all
import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isInstanceOf
import assertk.assertions.isNotNull
import assertk.assertions.isNull
import io.mockk.impl.annotations.RelaxedMockK
import io.mockk.spyk
import io.qalipsis.api.steps.StepCreationContext
import io.qalipsis.api.steps.StepCreationContextImpl
import io.qalipsis.plugins.kafka.producer.KafkaProducerStep
import io.qalipsis.test.assertk.prop
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.qalipsis.test.mockk.WithMockk
import io.qalipsis.test.mockk.relaxedMockk
import io.qalipsis.test.steps.AbstractStepSpecificationConverterTest
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Serializer
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension

/**
 * @author Gabriel Moraes
 */
@WithMockk
@Suppress("UNCHECKED_CAST")
internal class KafkaProducerStepSpecificationConverterTest: AbstractStepSpecificationConverterTest<KafkaProducerStepSpecificationConverter<Any, Any>>() {

    @JvmField
    @RegisterExtension
    val testDispatcherProvider = TestDispatcherProvider()

    @RelaxedMockK
    private lateinit var keySerializer: Serializer<Any>

    @RelaxedMockK
    private lateinit var valueSerializer: Serializer<Any>

    @Test
    override fun `should not support unexpected spec`() {
        Assertions.assertFalse(converter.support(relaxedMockk()))
    }

    @Test
    override fun `should support expected spec`() {
        Assertions.assertTrue(converter.support(relaxedMockk<KafkaProducerStepSpecificationImpl<*, *, *>>()))
    }

    @Test
    internal fun `should convert spec with name and retry policy`() = testDispatcherProvider.runTest {
        // given
        val spec = KafkaProducerStepSpecificationImpl<Any, Any, Any>(keySerializer, valueSerializer)
        spec.apply {
            name = "my-step"
            retryPolicy = mockedRetryPolicy
            bootstrap("localhost:9092", "localhost:9093")
            clientName("test")
            monitoring {
                events = true
                meters = true
            }
            properties(mapOf(ProducerConfig.LINGER_MS_CONFIG to 10))
            records { _, _ ->
                listOf()
            }
        }
        val creationContext = StepCreationContextImpl(scenarioSpecification, directedAcyclicGraph, spec)

        val spiedConverter = spyk(converter)


        // when
        spiedConverter.convert<Unit, Map<String, *>>(
            creationContext as StepCreationContext<KafkaProducerStepSpecificationImpl<*, Any, Any>>
        )

        // then
        creationContext.createdStep!!.let {
            assertThat(it).isInstanceOf(KafkaProducerStep::class).all {
                prop("stepId").isEqualTo("my-step")
                prop("clientName").isEqualTo("test")
                prop("props").isNotNull()
                prop("retryPolicy").isNotNull()
                prop("recordsFactory").isNotNull()
                prop("keySerializer").isEqualTo(keySerializer)
                prop("valuesSerializer").isEqualTo(valueSerializer)
            }
        }
    }


    @Test
    internal fun `should convert spec without name and retry policy`() = testDispatcherProvider.runTest {
        // given
        val spec = KafkaProducerStepSpecificationImpl<Any, Any, Any>(keySerializer, valueSerializer)
        spec.apply {
            bootstrap("localhost:9092", "localhost:9093")
            clientName("test")
            monitoring {
                events = true
                meters = true
            }
            properties(mapOf(ProducerConfig.LINGER_MS_CONFIG to 10))
            records { _, _ ->
                listOf()
            }
        }
        val creationContext = StepCreationContextImpl(scenarioSpecification, directedAcyclicGraph, spec)

        val spiedConverter = spyk(converter)

        // when
        spiedConverter.convert<Unit, Map<String, *>>(
            creationContext as StepCreationContext<KafkaProducerStepSpecificationImpl<*, Any, Any>>
        )

        // then
        creationContext.createdStep!!.let {
            assertThat(it).isInstanceOf(KafkaProducerStep::class).all {
                prop("clientName").isEqualTo("test")
                prop("props").isNotNull()
                prop("retryPolicy").isNull()
                prop("recordsFactory").isNotNull()
                prop("meterRegistry").isNotNull()
                prop("eventsLogger").isNotNull()
                prop("keySerializer").isEqualTo(keySerializer)
                prop("valuesSerializer").isEqualTo(valueSerializer)
            }
        }
    }
}