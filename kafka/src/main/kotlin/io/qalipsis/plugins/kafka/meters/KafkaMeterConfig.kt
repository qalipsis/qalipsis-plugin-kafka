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

package io.qalipsis.plugins.kafka.meters

import io.micronaut.context.annotation.ConfigurationProperties
import io.micronaut.context.annotation.Requires
import io.micronaut.context.env.Environment
import io.micronaut.core.util.StringUtils
import io.qalipsis.api.config.MetersConfig
import io.qalipsis.plugins.kafka.meters.KafkaMeterConfig.Companion.KAFKA_CONFIGURATION
import io.qalipsis.plugins.kafka.meters.KafkaMeterConfig.Companion.KAFKA_ENABLED
import org.apache.kafka.clients.producer.ProducerConfig
import java.util.Properties
import javax.validation.constraints.NotBlank
import kotlin.jvm.optionals.getOrNull


/**
 * Configuration properties for Kafka.
 *
 * @author Palina Bril
 */
@Requires(property = KAFKA_ENABLED, value = StringUtils.TRUE)
@ConfigurationProperties(KAFKA_CONFIGURATION)
internal class KafkaMeterConfig(private val environment: Environment) {

    @get:NotBlank
    var prefix: String = "qalipsis"

    @get:NotBlank
    var topic: String = "qalipsis-meters"

    /**
     * The name of the timestamp field. Default is: "timestamp"
     *
     * @return field name for timestamp
     */
    @get:NotBlank
    var timestampFieldName: String = "timestamp"

    fun configuration(): Properties {

        val properties = Properties()
        properties[ProducerConfig.BATCH_SIZE_CONFIG] =
            environment.getProperty("${KAFKA_CONFIGURATION}.batch-size", Int::class.java).getOrNull() ?: 1
        properties[ProducerConfig.LINGER_MS_CONFIG] =
            environment.getProperty("${KAFKA_CONFIGURATION}.linger.ms", String::class.java).getOrNull() ?: "1000"
        ProducerConfig.configNames().forEach { producerConfigKey ->
            environment.getProperty("${KAFKA_CONFIGURATION}.configuration.$producerConfigKey", String::class.java)
                .orElse(null)?.let { configValue ->
                    properties[producerConfigKey] = configValue
                }
        }
        properties[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] =
            environment.getProperty("${KAFKA_CONFIGURATION}.bootstrap.servers", String::class.java).getOrNull()
                ?: "localhost:9092"
        return properties
    }

    companion object {

        const val KAFKA_CONFIGURATION = "${MetersConfig.EXPORT_CONFIGURATION}.kafka"

        const val KAFKA_ENABLED = "$KAFKA_CONFIGURATION.enabled"
    }
}
