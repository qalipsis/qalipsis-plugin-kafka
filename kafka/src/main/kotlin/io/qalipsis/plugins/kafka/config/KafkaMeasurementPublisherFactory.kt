/*
 * Copyright 2024 AERIS IT Solutions GmbH
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

package io.qalipsis.plugins.kafka.config

import io.micronaut.context.annotation.Requirements
import io.micronaut.context.annotation.Requires
import io.micronaut.context.env.Environment
import io.micronaut.core.util.StringUtils
import io.qalipsis.api.config.MetersConfig
import io.qalipsis.api.meters.MeasurementPublisher
import io.qalipsis.api.meters.MeasurementPublisherFactory
import io.qalipsis.plugins.kafka.meters.KafkaMeasurementPublisher
import io.qalipsis.plugins.kafka.meters.KafkaMeterConfig
import jakarta.inject.Singleton

/**
 * Configuration for the export of Qalipsis meters to Kafka.
 *
 * @author Francisca Eze
 */
@Singleton
@Requirements(
    Requires(property = MetersConfig.EXPORT_ENABLED, value = StringUtils.TRUE),
    Requires(property = KafkaMeterConfig.KAFKA_ENABLED, value = StringUtils.TRUE)
)
internal class KafkaMeasurementPublisherFactory(
    private val configuration: KafkaMeterConfig
) : MeasurementPublisherFactory {
    override fun getPublisher(): MeasurementPublisher {
        return KafkaMeasurementPublisher(configuration)
    }

}
