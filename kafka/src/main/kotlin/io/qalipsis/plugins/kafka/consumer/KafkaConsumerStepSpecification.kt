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

import io.qalipsis.api.annotations.Spec
import io.qalipsis.api.constraints.PositiveOrZeroDuration
import io.qalipsis.api.scenario.StepSpecificationRegistry
import io.qalipsis.api.steps.AbstractStepSpecification
import io.qalipsis.api.steps.ConfigurableStepSpecification
import io.qalipsis.api.steps.SingletonConfiguration
import io.qalipsis.api.steps.SingletonStepSpecification
import io.qalipsis.api.steps.SingletonType
import io.qalipsis.api.steps.StepMonitoringConfiguration
import io.qalipsis.api.steps.StepSpecification
import io.qalipsis.api.steps.UnicastSpecification
import io.qalipsis.plugins.kafka.KafkaScenarioSpecification
import io.qalipsis.plugins.kafka.KafkaStepSpecification
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serdes
import java.time.Duration
import java.util.regex.Pattern
import javax.validation.constraints.NotBlank
import javax.validation.constraints.Positive
import kotlin.reflect.KClass

interface KafkaConsumerConfigurableSpecification<K : Any, V : Any> : UnicastSpecification,
    ConfigurableStepSpecification<Unit, List<KafkaConsumerResult<K?, V?>>, KafkaDeserializerSpecification<K, V>> {

    /**
     * Configure the bootstrap hosts of the Kafka cluster, default to localhost:9092.
     */
    fun bootstrap(vararg hosts: String)

    /**
     * Defines the list of topics to consume, exclusive with [topicsPattern].
     */
    fun topics(vararg topics: String)

    /**
     * Defines the regex pattern of topics to consume, exclusive with [topics].
     */
    fun topicsPattern(topicsPattern: Pattern)

    /**
     * Defines the consumer poll timeout to wait for records, default to 1 second.
     */
    fun pollTimeout(pollTimeout: Duration)

    /**
     * Defines the consumer poll timeout in milliseconds to wait for records, default to 1 second.
     */
    fun pollTimeout(pollTimeout: Long)

    /**
     * Defines the strategy to apply when the consumer group is not known from the cluster, default to [OffsetResetStrategy.LATEST].
     */
    fun offsetReset(offsetReset: OffsetResetStrategy)

    /**
     * Defines the maximal number of records returned in a unique poll batch, default to 500.
     */
    fun maxPolledRecords(maxPolledRecords: Int)

    /**
     * Consumer group, no default.
     */
    fun groupId(groupId: String)

    /**
     * Number of concurrent threads consuming the topics for the same group, default to 1.
     */
    fun concurrency(concurrency: Int)

    /**
     * Additional properties for the consumer, as documented [here](https://kafka.apache.org/documentation/#consumerconfigs).
     */
    fun properties(vararg properties: Pair<String, Any>)

    /**
     * Additional properties for the consumer, as documented [here](https://kafka.apache.org/documentation/#consumerconfigs).
     */
    fun properties(properties: Map<String, Any>)

    /**
     * Configures the monitoring of the consumer step.
     */
    fun monitoring(monitoringConfig: StepMonitoringConfiguration.() -> Unit)
}

interface KafkaDeserializerSpecification<K : Any, V : Any> :
    StepSpecification<Unit, List<KafkaConsumerResult<K?, V?>>, KafkaDeserializerSpecification<K, V>> {

    /**
     * Uses an instance of [keyDeserializer] to deserialize the keys and [valueDeserializer] for the values.
     * Both classes have to inherit from [Deserializer].
     */
    fun <K1 : Any, V1 : Any> deserialize(
        keyDeserializer: String,
        valueDeserializer: String
    ): StepSpecification<Unit, List<KafkaConsumerResult<K1?, V1?>>, *>

    /**
     * Uses an instance of [keyDeserializer] to deserialize the keys and [valueDeserializer] for the values.
     * Both classes have to inherit from [Deserializer].
     */
    fun <K1 : Any, V1 : Any> deserialize(
        keyDeserializer: KClass<out Deserializer<K1>>,
        valueDeserializer: KClass<out Deserializer<V1>>
    ): StepSpecification<Unit, List<KafkaConsumerResult<K1?, V1?>>, *>

    /**
     * Uses [keyDeserializer] to deserialize the keys and [valueDeserializer] for the values.
     */
    fun <K1 : Any, V1 : Any> deserialize(
        keyDeserializer: Deserializer<K1>,
        valueDeserializer: Deserializer<V1>
    ): StepSpecification<Unit, List<KafkaConsumerResult<K1?, V1?>>, *>

    /**
     * Uses an instance of [keyDeserializer] to deserialize the keys and [valueDeserializer] for the values
     * and returns each value independently instead of the whole batch.
     *
     * Both classes have to inherit from [Deserializer].
     *
     * Use a [KafkaDeserializerWrapper] to use native Kafka [Deserializer]s.
     */
    fun <K1 : Any, V1 : Any> flatten(keyDeserializer: String,
                                     valueDeserializer: String): StepSpecification<Unit, KafkaConsumerRecord<K1?, V1?>, *>

    /**
     * Uses an instance of [keyDeserializer] to deserialize the keys and [valueDeserializer] for the values
     * and returns each value independently instead of the whole batch.
     *
     * Both classes have to inherit from [Deserializer].
     *
     * Use a [KafkaDeserializerWrapper] to use native Kafka [Deserializer]s.
     */
    fun <K1 : Any, V1 : Any> flatten(
        keyDeserializer: KClass<out Deserializer<K1>>,
        valueDeserializer: KClass<out Deserializer<V1>>
    ): StepSpecification<Unit, KafkaConsumerResult<K1?, V1?>, *>

    /**
     * Uses [keyDeserializer] to deserialize the keys and [valueDeserializer] for the values
     * and returns each value independently instead of the whole batch.
     *
     * Use a [KafkaDeserializerWrapper] to use native Kafka [Deserializer]s.
     */
    fun <K1 : Any, V1 : Any> flatten(
        keyDeserializer: Deserializer<K1>,
        valueDeserializer: Deserializer<V1>
    ): StepSpecification<Unit, KafkaConsumerResult<K1?, V1?>, *>
}

/**
 * Specification to open a Kafka stream.
 *
 * @author Eric Jessé
 */
@Spec
internal class KafkaConsumerStepSpecification<K : Any, V : Any> internal constructor(
    keyDeserializer: Deserializer<K>,
    valueDeserializer: Deserializer<V>
) : AbstractStepSpecification<Unit, List<KafkaConsumerResult<K?, V?>>, KafkaDeserializerSpecification<K, V>>(),
    KafkaConsumerConfigurableSpecification<K, V>,
    KafkaDeserializerSpecification<K, V>,
    KafkaStepSpecification<Unit, List<KafkaConsumerResult<K?, V?>>, KafkaDeserializerSpecification<K, V>>,
    SingletonStepSpecification {

    internal var monitoringConfig = StepMonitoringConfiguration()
    internal val configuration =
        KafkaConsumerConfiguration(keyDeserializer = keyDeserializer, valueDeserializer = valueDeserializer)

    override val singletonConfiguration: SingletonConfiguration = SingletonConfiguration(SingletonType.UNICAST)

    override fun bootstrap(vararg hosts: String) {
        configuration.bootstrap = hosts.joinToString(",")
    }

    override fun topics(vararg topics: String) {
        configuration.topics.clear()
        configuration.topicsPattern = null
        configuration.topics.addAll(topics.toList())
    }

    override fun topicsPattern(topicsPattern: Pattern) {
        configuration.topics.clear()
        configuration.topicsPattern = topicsPattern
    }

    override fun offsetReset(offsetReset: OffsetResetStrategy) {
        configuration.offsetReset = offsetReset
    }

    override fun pollTimeout(pollTimeout: Duration) {
        configuration.pollTimeout = pollTimeout
    }

    override fun pollTimeout(pollTimeout: Long) {
        configuration.pollTimeout = Duration.ofMillis(pollTimeout)
    }

    override fun maxPolledRecords(maxPolledRecords: Int) {
        configuration.maxPolledRecords = maxPolledRecords
    }

    override fun groupId(groupId: String) {
        configuration.groupId = groupId
    }

    override fun concurrency(concurrency: Int) {
        configuration.concurrency = concurrency
    }

    override fun properties(vararg properties: Pair<String, Any>) {
        configuration.properties.putAll(properties)
    }

    override fun properties(properties: Map<String, Any>) {
        configuration.properties.putAll(properties)
    }

    override fun monitoring(monitoringConfig: StepMonitoringConfiguration.() -> Unit) {
        this.monitoringConfig.monitoringConfig()
    }

    override fun unicast(bufferSize: Int, idleTimeout: Duration) {
        singletonConfiguration.bufferSize = bufferSize
        singletonConfiguration.idleTimeout = idleTimeout
    }

    @Suppress("UNCHECKED_CAST")
    override fun <K1 : Any, V1 : Any> deserialize(
        keyDeserializer: String,
        valueDeserializer: String
    ): StepSpecification<Unit, List<KafkaConsumerResult<K1?, V1?>>, *> {
        this as KafkaConsumerStepSpecification<K1, V1>
        configuration.keyDeserializer =
            (Class.forName(keyDeserializer) as Class<Deserializer<K1>>).getDeclaredConstructor().newInstance()
        configuration.valueDeserializer =
            (Class.forName(valueDeserializer) as Class<Deserializer<V1>>).getDeclaredConstructor().newInstance()
        return this
    }

    @Suppress("UNCHECKED_CAST")
    override fun <K1 : Any, V1 : Any> deserialize(
        keyDeserializer: KClass<out Deserializer<K1>>,
        valueDeserializer: KClass<out Deserializer<V1>>
    ): StepSpecification<Unit, List<KafkaConsumerResult<K1?, V1?>>, *> {
        this as KafkaConsumerStepSpecification<K1, V1>
        // Use the Java classes to avoid the need of the Kotlin reflection API.
        configuration.keyDeserializer = keyDeserializer.java.getDeclaredConstructor().newInstance()
        configuration.valueDeserializer = valueDeserializer.java.getDeclaredConstructor().newInstance()
        return this
    }

    @Suppress("UNCHECKED_CAST")
    override fun <K1 : Any, V1 : Any> deserialize(
        keyDeserializer: Deserializer<K1>,
        valueDeserializer: Deserializer<V1>
    ): StepSpecification<Unit, List<KafkaConsumerResult<K1?, V1?>>, *> {
        this as KafkaConsumerStepSpecification<K1, V1>
        configuration.keyDeserializer = keyDeserializer
        configuration.valueDeserializer = valueDeserializer
        return this
    }

    @Suppress("UNCHECKED_CAST")
    override fun <K1 : Any, V1 : Any> flatten(keyDeserializer: String,
                                              valueDeserializer: String): StepSpecification<Unit, KafkaConsumerRecord<K1?, V1?>, *> {
        configuration.flattenOutput = true
        this as KafkaConsumerStepSpecification<K1, V1>
        deserialize<K1, V1>(keyDeserializer, valueDeserializer)
        return this as StepSpecification<Unit, KafkaConsumerRecord<K1?, V1?>, *>
    }

    @Suppress("UNCHECKED_CAST")
    override fun <K1 : Any, V1 : Any> flatten(
        keyDeserializer: KClass<out Deserializer<K1>>,
        valueDeserializer: KClass<out Deserializer<V1>>
    ): StepSpecification<Unit, KafkaConsumerResult<K1?, V1?>, *> {
        configuration.flattenOutput = true
        this as KafkaConsumerStepSpecification<K1, V1>
        deserialize(keyDeserializer, valueDeserializer)
        return this as StepSpecification<Unit, KafkaConsumerResult<K1?, V1?>, *>
    }

    @Suppress("UNCHECKED_CAST")
    override fun <K1 : Any, V1 : Any> flatten(
        keyDeserializer: Deserializer<K1>,
        valueDeserializer: Deserializer<V1>
    ): StepSpecification<Unit, KafkaConsumerResult<K1?, V1?>, *> {
        configuration.flattenOutput = true
        this as KafkaConsumerStepSpecification<K1, V1>
        deserialize(keyDeserializer, valueDeserializer)
        return this as StepSpecification<Unit, KafkaConsumerResult<K1?, V1?>, *>
    }
}

@Spec
internal data class KafkaConsumerConfiguration<K, V>(
    internal var bootstrap: @NotBlank String = "localhost:9092",
    internal var topics: MutableList<@NotBlank String> = mutableListOf(),
    internal var topicsPattern: Pattern? = null,
    @PositiveOrZeroDuration internal var pollTimeout: Duration = Duration.ofSeconds(1),
    internal var offsetReset: OffsetResetStrategy = OffsetResetStrategy.LATEST,
    internal var maxPolledRecords: @Positive Int = 500,
    internal var groupId: @NotBlank String = "",
    internal var keyDeserializer: Deserializer<K>,
    internal var valueDeserializer: Deserializer<V>,
    internal var concurrency: @Positive Int = 1,
    internal var properties: MutableMap<@NotBlank String, Any> = mutableMapOf(),
    internal var flattenOutput: Boolean = false
)


/**
 * Creates a Kafka consumers to poll data from topics of an Apache Kafka cluster and forward each message to the next step, either as batches or individually.
 *
 * This step is generally used in conjunction with a left join to assert data or inject them in a workflow.
 *
 * You can learn more on [Apache Kafka website](https://kafka.apache.org).
 *
 * @author Eric Jessé
 */
fun KafkaScenarioSpecification.consume(
    configurationBlock: KafkaConsumerConfigurableSpecification<ByteArray, ByteArray>.() -> Unit
): KafkaDeserializerSpecification<ByteArray, ByteArray> {
    val defaultDeserializer = Serdes.ByteArray().deserializer()
    val step = KafkaConsumerStepSpecification(defaultDeserializer, defaultDeserializer)
    step.configurationBlock()
    (this as StepSpecificationRegistry).add(step)
    return step
}