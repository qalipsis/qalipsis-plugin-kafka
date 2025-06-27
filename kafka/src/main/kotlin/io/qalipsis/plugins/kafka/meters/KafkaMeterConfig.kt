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
import javax.validation.constraints.Positive
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

    @get:NotBlank
    var serializer: String = "json"

    @get:Positive
    var lingerMs: Long = 1000

    fun configuration(): Properties {

        val properties = Properties()
        properties[ProducerConfig.BATCH_SIZE_CONFIG] =
            environment.getProperty("${KAFKA_CONFIGURATION}.batch-size", Int::class.java).getOrNull() ?: 1
        properties[ProducerConfig.LINGER_MS_CONFIG] =
            environment.getProperty("${KAFKA_CONFIGURATION}.linger.ms", String::class.java).getOrNull()
                ?: lingerMs.toString()
        ProducerConfig.configNames().forEach { producerConfigKey ->
            environment.getProperty("${KAFKA_CONFIGURATION}.configuration.$producerConfigKey", String::class.java)
                .ifPresent { configValue ->
                    properties[producerConfigKey] = configValue
                }
        }
        properties[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] =
            environment.getProperty("${KAFKA_CONFIGURATION}.bootstrap", String::class.java).getOrNull()
                ?: "localhost:9092"
        return properties
    }

    companion object {

        const val KAFKA_CONFIGURATION = "${MetersConfig.EXPORT_CONFIGURATION}.kafka"

        const val KAFKA_ENABLED = "$KAFKA_CONFIGURATION.enabled"
    }
}
