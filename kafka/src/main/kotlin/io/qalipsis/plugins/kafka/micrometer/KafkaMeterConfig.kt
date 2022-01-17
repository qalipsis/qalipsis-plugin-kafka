package io.qalipsis.plugins.kafka.micrometer

import io.micrometer.core.instrument.config.MeterRegistryConfigValidator
import io.micrometer.core.instrument.config.validate.PropertyValidator
import io.micrometer.core.instrument.config.validate.Validated
import io.micrometer.core.instrument.step.StepRegistryConfig
import org.apache.kafka.clients.producer.ProducerConfig
import java.util.Properties


/**
 * {@link MeterRegistry} for Kafka
 *
 * @author Palina Bril
 */
abstract class KafkaMeterConfig : StepRegistryConfig {

    override fun prefix(): String? {
        return "kafka"
    }

    fun configuration(): Properties {
        val properties = Properties()
        properties[ProducerConfig.BATCH_SIZE_CONFIG] = batchSize()
        properties[ProducerConfig.LINGER_MS_CONFIG] = lingerMs()
        ProducerConfig.configNames().forEach { producerConfigKey ->
            PropertyValidator.getString(this, "configuration.${producerConfigKey}").orElse(null)?.let { configValue ->
                properties[producerConfigKey] = configValue
            }
        }

        PropertyValidator.getString(this, ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)
            .orElse("localhost:9092")?.let { configValue ->
                properties[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = configValue
            }

        return properties
    }

    fun topic(): String {
        return PropertyValidator.getString(this, "topic").orElse("qalipsis-meters")
    }

    private fun lingerMs(): String {
        return PropertyValidator.getInteger(this, ProducerConfig.LINGER_MS_CONFIG).orElse(1000).toString()
    }

    /**
     * The name of the timestamp field. Default is: "timestamp"
     *
     * @return field name for timestamp
     */
    fun timestampFieldName(): String {
        return PropertyValidator.getString(this, "timestampFieldName").orElse("timestamp")
    }

    override fun validate(): Validated<*>? {
        return MeterRegistryConfigValidator.checkAll(this,
            { c: KafkaMeterConfig -> StepRegistryConfig.validate(c) },
            MeterRegistryConfigValidator.checkRequired("topic") { obj: KafkaMeterConfig -> obj.topic() }
        )
    }
}
