package io.qalipsis.plugins.kafka.producer

import assertk.all
import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.isEmpty
import assertk.assertions.isEqualTo
import assertk.assertions.isFalse
import assertk.assertions.isInstanceOf
import assertk.assertions.isNotNull
import assertk.assertions.isTrue
import assertk.assertions.prop
import io.aerisconsulting.catadioptre.getProperty
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.steps.DummyStepSpecification
import io.qalipsis.api.steps.StepMonitoringConfiguration
import io.qalipsis.plugins.kafka.kafka
import io.qalipsis.test.mockk.relaxedMockk
import kotlinx.coroutines.test.runBlockingTest
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Serializer
import org.junit.jupiter.api.Test

/**
 * @author Gabriel Moraes
 */
internal class KafkaProducerStepSpecificationImplTest {

    @Test
    fun `should add minimal configuration for the step`() = runBlockingTest {
        val previousStep = DummyStepSpecification()
        val keySerializer = relaxedMockk<Serializer<Any>>()
        val valueSerializer = relaxedMockk<Serializer<Any>>()
        previousStep.kafka().produce(keySerializer, valueSerializer) {
            name = "producer-step"
            bootstrap("localhost:9092")
            clientName("test")
            records { _, _ ->
                listOf(KafkaProducerRecord(topic = "records", value = "1"))
            }
        }

        val nextStep = previousStep.nextSteps[0]
        assertThat(nextStep).isInstanceOf(KafkaProducerStepSpecificationImpl::class).all {
            prop(KafkaProducerStepSpecificationImpl<*, *, *>::configuration).all {
                prop(KafkaProducerConfiguration<*, *, *>::bootstrap).isEqualTo("localhost:9092")
                prop(KafkaProducerConfiguration<*, *, *>::clientName).isEqualTo("test")
                prop(KafkaProducerConfiguration<*, *, *>::keySerializer).isEqualTo(keySerializer)
                prop(KafkaProducerConfiguration<*, *, *>::valueSerializer).isEqualTo(valueSerializer)
                prop(KafkaProducerConfiguration<*, *, *>::properties).isEmpty()
                prop(KafkaProducerConfiguration<*, *, *>::recordsFactory).isNotNull()
                prop(KafkaProducerConfiguration<*, *, *>::metricsConfiguration).all {
                    prop(KafkaProducerMetricsConfiguration::recordsCount).isFalse()
                    prop(KafkaProducerMetricsConfiguration::keysBytesSent).isFalse()
                    prop(KafkaProducerMetricsConfiguration::valuesBytesSent).isFalse()
                }
            }
        }

        val recordsFactory = nextStep.getProperty<KafkaProducerConfiguration<*, *, *>>("configuration")
            .getProperty<suspend (ctx: StepContext<*, *>, input: Int) -> List<KafkaProducerRecord<*, *>>>("recordsFactory")
        assertThat(recordsFactory(relaxedMockk(), relaxedMockk())).isEqualTo(
            listOf(
                KafkaProducerRecord<Any, Any>(topic = "records", value = "1")
            )
        )
    }


    @Test
    fun `should add a complete configuration for the step`() = runBlockingTest {
        val previousStep = DummyStepSpecification()
        val keySerializer = relaxedMockk<Serializer<Any>>()
        val valueSerializer = relaxedMockk<Serializer<Any>>()
        previousStep.kafka().produce(keySerializer, valueSerializer) {
            name = "producer-step"
            bootstrap("localhost:9092", "localhost:9093")
            clientName("test")
            monitoring {
                events = true
                meters = true
            }
            properties(mapOf(ProducerConfig.LINGER_MS_CONFIG to 10))
            records { _, _ ->
                listOf(
                    KafkaProducerRecord(topic = "records", value = "1"),
                    KafkaProducerRecord(topic = "records", value = "2")
                )
            }
        }

        val nextStep = previousStep.nextSteps[0]
        assertThat(nextStep).isInstanceOf(KafkaProducerStepSpecificationImpl::class).all {
            prop(KafkaProducerStepSpecificationImpl<*, *, *>::configuration).all {
                prop(KafkaProducerConfiguration<*, *, *>::bootstrap).isEqualTo("localhost:9092,localhost:9093")
                prop(KafkaProducerConfiguration<*, *, *>::clientName).isEqualTo("test")
                prop(KafkaProducerConfiguration<*, *, *>::keySerializer).isEqualTo(keySerializer)
                prop(KafkaProducerConfiguration<*, *, *>::valueSerializer).isEqualTo(valueSerializer)
                prop(KafkaProducerConfiguration<*, *, *>::properties).all {
                    hasSize(1)
                    isEqualTo(mapOf(ProducerConfig.LINGER_MS_CONFIG to 10))
                }
                prop(KafkaProducerConfiguration<*, *, *>::recordsFactory).isNotNull()
            }
            transform { it.monitoringConfig }.all {
                prop(StepMonitoringConfiguration::events).isTrue()
                prop(StepMonitoringConfiguration::meters).isTrue()
            }
        }

        val recordsFactory = nextStep.getProperty<KafkaProducerConfiguration<*, *, *>>("configuration")
            .getProperty<suspend (ctx: StepContext<*, *>, input: Int) -> List<KafkaProducerRecord<*, *>>>("recordsFactory")

        assertThat(recordsFactory(relaxedMockk(), relaxedMockk())).hasSize(2)
    }

}