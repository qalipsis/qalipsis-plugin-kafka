/*
 * Copyright 2022 AERIS IT Solutions GmbH
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

package io.qalipsis.plugins.kafka.consumer

import io.qalipsis.api.annotations.StepConverter
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.meters.CampaignMeterRegistry
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
    private val meterRegistry: CampaignMeterRegistry,
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