package io.qalipsis.plugins.kafka.config

import io.micrometer.core.instrument.Clock
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requirements
import io.micronaut.context.annotation.Requires
import io.micronaut.context.env.Environment
import io.micronaut.core.naming.conventions.StringConvention
import io.micronaut.core.util.StringUtils
import io.qalipsis.api.config.MetersConfig
import io.qalipsis.plugins.kafka.meters.KafkaMeterConfig
import io.qalipsis.plugins.kafka.meters.KafkaMeterRegistry
import jakarta.inject.Singleton
import java.util.Properties

/**
 * Configuration for the export of micrometer [io.micrometer.core.instrument.Meter] to Kafka.
 *
 * @author Palina Bril
 */
@Factory
@Requirements(
    Requires(property = MetersConfig.EXPORT_ENABLED, notEquals = StringUtils.FALSE),
    Requires(property = KafkaMeterRegistryFactory.KAFKA_ENABLED, notEquals = StringUtils.FALSE)
)
internal class KafkaMeterRegistryFactory {

    @Singleton
    fun kafkaRegistry(environment: Environment): KafkaMeterRegistry {
        val properties = Properties()
        properties.putAll(environment.getProperties(MetersConfig.EXPORT_CONFIGURATION, StringConvention.RAW))
        properties.putAll(environment.getProperties(MetersConfig.EXPORT_CONFIGURATION, StringConvention.CAMEL_CASE))

        return KafkaMeterRegistry(
            object : KafkaMeterConfig() {
                override fun get(key: String): String? {
                    return properties[key]?.toString()
                }
            },
            Clock.SYSTEM
        )
    }

    companion object {

        private const val KAFKA_CONFIGURATION = "${MetersConfig.EXPORT_CONFIGURATION}.kafka"

        internal const val KAFKA_ENABLED = "$KAFKA_CONFIGURATION.enabled"
    }
}
