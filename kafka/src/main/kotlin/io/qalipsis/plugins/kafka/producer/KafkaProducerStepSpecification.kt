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

import io.qalipsis.api.annotations.Spec
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.steps.AbstractStepSpecification
import io.qalipsis.api.steps.ConfigurableStepSpecification
import io.qalipsis.api.steps.StepMonitoringConfiguration
import io.qalipsis.plugins.kafka.KafkaStepSpecification
import org.apache.kafka.common.serialization.Serializer
import javax.validation.constraints.NotBlank

/**
 * Specification for a [KafkaProducerStep] to produce data onto a Kafka topic.
 *
 * The output is a [KafkaProducerResult] that contains the output from previous step and metrics regarding this step.
 *
 * @author Gabriel Moraes
 */
interface KafkaProducerStepSpecification<I, K, V> :
    ConfigurableStepSpecification<I, KafkaProducerResult<I>, KafkaProducerStepSpecification<I, K, V>> {

    /**
     * Configures the bootstrap of hosts for the Kafka cluster, defaults to localhost:9092.
     */
    fun bootstrap(vararg hosts: String)

    /**
     * Defines the name of the client used in the kafka client.
     */
    fun clientName(clientName: String)

    /**
     * Configures additional properties for the producer, as documented [here](https://kafka.apache.org/documentation/#producerconfigs).
     */
    fun properties(vararg properties: Pair<String, Any>)

    /**
     * Configures additional properties for the producer, as documented [here](https://kafka.apache.org/documentation/#producerconfigs).
     */
    fun properties(properties: Map<String, Any>)

    /**
     * Defines the records to be published, it receives the context and the output from previous step that can be used
     * when defining the records.
     */
    fun records(recordsConfiguration: suspend (stepContext: StepContext<*, *>, input: I) -> List<KafkaProducerRecord<K, V>>)

    /**
     * Configures the monitoring of the producer step.
     */
    fun monitoring(monitoringConfig: StepMonitoringConfiguration.() -> Unit)
}
/**
 * Specification to a Kafka publisher, implementation of [KafkaProducerStepSpecification].
 *
 * @author Gabriel Moraes
 */
@Spec
internal class KafkaProducerStepSpecificationImpl<I, K, V>(
    keySerializer: Serializer<K>,
    valueSerializer: Serializer<V>
) : AbstractStepSpecification<I, KafkaProducerResult<I>, KafkaProducerStepSpecification<I, K, V>>(),
    KafkaProducerStepSpecification<I, K, V>,
    KafkaStepSpecification<I, KafkaProducerResult<I>, KafkaProducerStepSpecification<I, K, V>> {

    internal var monitoringConfig = StepMonitoringConfiguration()
    internal val configuration = KafkaProducerConfiguration<I, K, V>(
        keySerializer = keySerializer,
        valueSerializer = valueSerializer
    )

    override fun bootstrap(vararg hosts: String) {
        configuration.bootstrap = hosts.joinToString(",")
    }

    override fun clientName(clientName: String) {
        configuration.clientName = clientName
    }

    override fun properties(vararg properties: Pair<String, Any>) {
        configuration.properties.putAll(properties)
    }

    override fun properties(properties: Map<String, Any>) {
        configuration.properties.putAll(properties)
    }

    override fun records(
        recordsConfiguration: suspend (stepContext: StepContext<*, *>, input: I) ->
        List<KafkaProducerRecord<K, V>>
    ) {
        configuration.recordsFactory = recordsConfiguration
    }

    override fun monitoring(monitoringConfig: StepMonitoringConfiguration.() -> Unit) {
        this.monitoringConfig.monitoringConfig()
    }

}

@Spec
internal data class KafkaProducerConfiguration<I, K, V>(
    @field:NotBlank internal var bootstrap: String = "localhost:9092",
    @field:NotBlank internal var clientName: String = "",
    internal var properties: MutableMap<String, Any> = mutableMapOf(),
    internal var keySerializer: Serializer<K>,
    internal var valueSerializer: Serializer<V>,
    internal var recordsFactory: (suspend (ctx: StepContext<*, *>, input: I) -> List<KafkaProducerRecord<K, V>>) = { _, _ ->
        emptyList()
    },
    internal var metricsConfiguration: KafkaProducerMetricsConfiguration = KafkaProducerMetricsConfiguration()
)

/**
 * Configuration of the metrics to record for the Kafka producer.
 *
 * @property keysBytesSent when true, records the number of bytes sent for the serialized keys.
 * @property valuesBytesSent when true, records the number of bytes sent for the serialized values.
 * @property recordsCount when true, records the number of sent messages.
 *
 * @author Gabriel Moraes
 */
@Spec
data class KafkaProducerMetricsConfiguration(
    var keysBytesSent: Boolean = false,
    var valuesBytesSent: Boolean = false,
    var recordsCount: Boolean = false,
)

/**
 * Creates a step to push data onto topics of a Kafka broker and forwards the input to the next step.
 *
 * You can learn more on [Apache Kafka website](https://kafka.apache.org).
 *
 * @author Gabriel Moraes
 */
fun <I, K, V> KafkaStepSpecification<*, I, *>.produce(
    keySerializer: Serializer<K>,
    valueSerializer: Serializer<V>,
    configurationBlock: KafkaProducerStepSpecification<I, K, V>.() -> Unit
): KafkaProducerStepSpecification<I, K, V> {
    val step = KafkaProducerStepSpecificationImpl<I, K, V>(keySerializer, valueSerializer)
    step.configurationBlock()
    this.add(step)
    return step
}