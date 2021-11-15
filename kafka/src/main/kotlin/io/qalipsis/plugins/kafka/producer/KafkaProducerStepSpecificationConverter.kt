package io.qalipsis.plugins.kafka.producer

import io.micrometer.core.instrument.MeterRegistry
import io.qalipsis.api.annotations.StepConverter
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.steps.StepCreationContext
import io.qalipsis.api.steps.StepSpecification
import io.qalipsis.api.steps.StepSpecificationConverter
import io.qalipsis.plugins.kafka.producer.KafkaProducerStep
import org.apache.kafka.clients.consumer.ConsumerConfig
import java.util.Properties

/**
 * [StepSpecificationConverter] from [KafkaProducerStepSpecificationImpl] to [KafkaProducerStep].
 *
 * @author Gabriel Moraes
 */
@StepConverter
internal class KafkaProducerStepSpecificationConverter<K, V>(
    private val meterRegistry: MeterRegistry,
    private val eventsLogger: EventsLogger

) : StepSpecificationConverter<KafkaProducerStepSpecificationImpl<*, K, V>> {

    override fun support(stepSpecification: StepSpecification<*, *, *>): Boolean {
        return stepSpecification is KafkaProducerStepSpecificationImpl<*, *, *>
    }

    override suspend fun <I, O> convert(creationContext: StepCreationContext<KafkaProducerStepSpecificationImpl<*, K, V>>) {
        val spec = creationContext.stepSpecification
        val configuration = spec.configuration

        val properties = buildAndGetProperties(configuration)
        val stepId = spec.name

        val step = KafkaProducerStep(
            stepId,
            spec.retryPolicy,
            configuration.clientName,
            properties,
            configuration.recordsFactory,
            configuration.keySerializer,
            configuration.valueSerializer,
            eventsLogger = eventsLogger.takeIf { spec.monitoringConfig.events },
            meterRegistry = meterRegistry.takeIf { spec.monitoringConfig.meters }
        )
        creationContext.createdStep(step)
    }

    private fun buildAndGetProperties(configuration: KafkaProducerConfiguration<*, K, V>): Properties {
        val properties = Properties()
        properties[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = configuration.bootstrap
        properties.putAll(configuration.properties)
        return properties
    }
}