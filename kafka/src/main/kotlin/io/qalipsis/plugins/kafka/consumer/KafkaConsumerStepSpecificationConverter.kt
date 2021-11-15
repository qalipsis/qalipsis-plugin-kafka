package io.qalipsis.plugins.kafka.consumer

import io.micrometer.core.instrument.MeterRegistry
import io.qalipsis.api.annotations.StepConverter
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.steps.StepCreationContext
import io.qalipsis.api.steps.StepMonitoringConfiguration
import io.qalipsis.api.steps.StepSpecification
import io.qalipsis.api.steps.StepSpecificationConverter
import io.qalipsis.api.steps.datasource.DatasourceObjectConverter
import io.qalipsis.api.steps.datasource.IterativeDatasourceStep
import io.qalipsis.api.steps.datasource.processors.NoopDatasourceObjectProcessor
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import java.util.*


/**
 * [StepSpecificationConverter] from [KafkaConsumerStepSpecification] to [KafkaConsumerIterativeReader] for a data source.
 *
 * @author Eric Jessé
 */
@StepConverter
internal class KafkaConsumerStepSpecificationConverter(
    private val meterRegistry: MeterRegistry,
    private val eventsLogger: EventsLogger
) : StepSpecificationConverter<KafkaConsumerStepSpecification<*, *>> {

    override fun support(stepSpecification: StepSpecification<*, *, *>): Boolean {
        return stepSpecification is KafkaConsumerStepSpecification<*, *>
    }

    override suspend fun <I, O> convert(creationContext: StepCreationContext<KafkaConsumerStepSpecification<*, *>>) {
        val spec = creationContext.stepSpecification
        val configuration = spec.configuration
        val properties = Properties()
        properties[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = configuration.bootstrap
        properties[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "${configuration.offsetReset}".lowercase()
        properties[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = configuration.maxPolledRecords
        properties[ConsumerConfig.GROUP_ID_CONFIG] = configuration.groupId
        properties.putAll(configuration.properties)

        val stepId = spec.name
        val reader = KafkaConsumerIterativeReader(
            stepId,
            properties,
            configuration.pollTimeout,
            configuration.concurrency,
            configuration.topics,
            configuration.topicsPattern
        )

        val step = IterativeDatasourceStep(
            stepId,
            reader,
            NoopDatasourceObjectProcessor(),
            buildConverter(spec.configuration, spec.monitoringConfig)
        )
        creationContext.createdStep(step)
    }

    private fun buildConverter(
        configuration: KafkaConsumerConfiguration<*, *>,
        monitoringConfiguration: StepMonitoringConfiguration
    ): DatasourceObjectConverter<ConsumerRecords<ByteArray?, ByteArray?>, out Any?> {

        return if (configuration.flattenOutput) {
            KafkaConsumerSingleConverter(
                configuration.keyDeserializer,
                configuration.valueDeserializer,
                eventsLogger = eventsLogger.takeIf { monitoringConfiguration.events },
                meterRegistry = meterRegistry.takeIf { monitoringConfiguration.meters }
            )
        } else {
            KafkaConsumerBatchConverter(
                configuration.keyDeserializer,
                configuration.valueDeserializer,
                eventsLogger = eventsLogger.takeIf { monitoringConfiguration.events },
                meterRegistry = meterRegistry.takeIf { monitoringConfiguration.meters }
            )
        }
    }


}