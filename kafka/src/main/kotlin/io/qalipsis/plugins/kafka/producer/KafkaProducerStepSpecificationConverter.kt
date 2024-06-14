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

package io.qalipsis.plugins.kafka.producer

import io.qalipsis.api.annotations.StepConverter
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.meters.CampaignMeterRegistry
import io.qalipsis.api.steps.StepCreationContext
import io.qalipsis.api.steps.StepSpecification
import io.qalipsis.api.steps.StepSpecificationConverter
import org.apache.kafka.clients.consumer.ConsumerConfig
import java.util.*

/**
 * [StepSpecificationConverter] from [KafkaProducerStepSpecificationImpl] to [KafkaProducerStep].
 *
 * @author Gabriel Moraes
 */
@StepConverter
internal class KafkaProducerStepSpecificationConverter<K, V>(
    private val meterRegistry: CampaignMeterRegistry,
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