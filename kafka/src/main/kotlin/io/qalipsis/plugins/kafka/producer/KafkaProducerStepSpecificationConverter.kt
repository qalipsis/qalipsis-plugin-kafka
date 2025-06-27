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

package io.qalipsis.plugins.kafka.producer

import io.qalipsis.api.annotations.StepConverter
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.meters.CampaignMeterRegistry
import io.qalipsis.api.steps.StepCreationContext
import io.qalipsis.api.steps.StepSpecification
import io.qalipsis.api.steps.StepSpecificationConverter
import org.apache.kafka.clients.consumer.ConsumerConfig
import java.util.Properties

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