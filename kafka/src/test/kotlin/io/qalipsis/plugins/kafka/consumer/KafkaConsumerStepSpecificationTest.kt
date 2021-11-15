package io.qalipsis.plugins.kafka.consumer

import assertk.all
import assertk.assertThat
import assertk.assertions.containsOnly
import assertk.assertions.hasSize
import assertk.assertions.isEqualTo
import assertk.assertions.isFalse
import assertk.assertions.isInstanceOf
import assertk.assertions.isNull
import assertk.assertions.isSameAs
import assertk.assertions.isTrue
import assertk.assertions.key
import assertk.assertions.prop
import io.qalipsis.api.scenario.StepSpecificationRegistry
import io.qalipsis.api.scenario.scenario
import io.qalipsis.api.steps.SingletonConfiguration
import io.qalipsis.api.steps.SingletonType
import io.qalipsis.api.steps.StepMonitoringConfiguration
import io.qalipsis.plugins.kafka.kafka
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.Deserializer
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.regex.Pattern
import kotlin.reflect.jvm.jvmName

/**
 *
 * @author Eric Jessé
 */
internal class KafkaConsumerStepSpecificationTest {

    @Test
    internal fun `should add minimal specification to the scenario`() {
        val scenario = scenario("my-scenario") as StepSpecificationRegistry
        scenario.kafka().consume {}

        assertThat(scenario.rootSteps[0]).isInstanceOf(KafkaConsumerStepSpecification::class).all {
            prop(KafkaConsumerStepSpecification<*, *>::configuration).all {
                prop(KafkaConsumerConfiguration<*, *>::bootstrap).isEqualTo("localhost:9092")
                prop(KafkaConsumerConfiguration<*, *>::concurrency).isEqualTo(1)
                prop(KafkaConsumerConfiguration<*, *>::topics).hasSize(0)
                prop(KafkaConsumerConfiguration<*, *>::topicsPattern).isNull()
                prop(KafkaConsumerConfiguration<*, *>::pollTimeout).isEqualTo(Duration.ofSeconds(1))
                prop(KafkaConsumerConfiguration<*, *>::maxPolledRecords).isEqualTo(500)
                prop(KafkaConsumerConfiguration<*, *>::offsetReset).isEqualTo(OffsetResetStrategy.LATEST)
                prop(KafkaConsumerConfiguration<*, *>::groupId).isEqualTo("")
                prop(KafkaConsumerConfiguration<*, *>::properties).hasSize(0)
                prop(KafkaConsumerConfiguration<*, *>::flattenOutput).isFalse()
                prop(KafkaConsumerConfiguration<*, *>::keyDeserializer).isInstanceOf(ByteArrayDeserializer::class)
                prop(KafkaConsumerConfiguration<*, *>::valueDeserializer).isInstanceOf(ByteArrayDeserializer::class)
            }

            transform { it.singletonConfiguration }.all {
                prop(SingletonConfiguration::type).isEqualTo(SingletonType.UNICAST)
                prop(SingletonConfiguration::bufferSize).isEqualTo(-1)
                prop(SingletonConfiguration::idleTimeout).isEqualTo(Duration.ZERO)
            }
        }
    }

    @Test
    internal fun `should apply user-defined configuration with topics, poll timeout as ms and events monitoring on`() {
        val scenario = scenario("my-scenario") as StepSpecificationRegistry
        scenario.kafka().consume {
            bootstrap("my-bootstrap")
            concurrency(2)
            topicsPattern(Pattern.compile(""))
            topics("topic-1", "topic-2") // This should set the topic pattern to null
            pollTimeout(3)
            maxPolledRecords(4)
            groupId("my-group")
            properties("key-1" to "value-1")
            properties("key-2" to "value-2", "key-3" to 5)
            offsetReset(OffsetResetStrategy.EARLIEST)

            forwardOnce(6, Duration.ofDays(1))
            monitoring {
                events = true
            }
        }

        assertThat(scenario.rootSteps[0]).isInstanceOf(KafkaConsumerStepSpecification::class).all {
            prop(KafkaConsumerStepSpecification<*, *>::configuration).all {
                prop(KafkaConsumerConfiguration<*, *>::bootstrap).isEqualTo("my-bootstrap")
                prop(KafkaConsumerConfiguration<*, *>::concurrency).isEqualTo(2)
                prop(KafkaConsumerConfiguration<*, *>::topics).containsOnly("topic-1", "topic-2")
                prop(KafkaConsumerConfiguration<*, *>::topicsPattern).isNull()
                prop(KafkaConsumerConfiguration<*, *>::pollTimeout).isEqualTo(Duration.ofMillis(3))
                prop(KafkaConsumerConfiguration<*, *>::maxPolledRecords).isEqualTo(4)
                prop(KafkaConsumerConfiguration<*, *>::offsetReset).isEqualTo(
                    OffsetResetStrategy.EARLIEST
                )
                prop(KafkaConsumerConfiguration<*, *>::groupId).isEqualTo("my-group")
                prop(KafkaConsumerConfiguration<*, *>::flattenOutput).isFalse()
                prop(KafkaConsumerConfiguration<*, *>::properties).all {
                    key("key-1").isEqualTo("value-1")
                    key("key-2").isEqualTo("value-2")
                    key("key-3").isEqualTo(5)
                }
                prop(KafkaConsumerConfiguration<*, *>::keyDeserializer).isInstanceOf(ByteArrayDeserializer::class)
                prop(KafkaConsumerConfiguration<*, *>::valueDeserializer).isInstanceOf(ByteArrayDeserializer::class)
            }
            transform { it.monitoringConfig }.all {
                prop(StepMonitoringConfiguration::events).isTrue()
                prop(StepMonitoringConfiguration::meters).isFalse()
            }
            transform { it.singletonConfiguration }.all {
                prop(SingletonConfiguration::type).isEqualTo(SingletonType.UNICAST)
                prop(SingletonConfiguration::bufferSize).isEqualTo(6)
                prop(SingletonConfiguration::idleTimeout).isEqualTo(Duration.ofDays(1))
            }
        }
    }

    @Test
    internal fun `should apply user-defined configuration with topics, poll timeout as ms and meters monitoring on`() {
        val scenario = scenario("my-scenario") as StepSpecificationRegistry
        scenario.kafka().consume {
            bootstrap("my-bootstrap")
            concurrency(2)
            topicsPattern(Pattern.compile(""))
            topics("topic-1", "topic-2") // This should set the topic pattern to null
            pollTimeout(3)
            maxPolledRecords(4)
            groupId("my-group")
            properties("key-1" to "value-1")
            properties("key-2" to "value-2", "key-3" to 5)
            offsetReset(OffsetResetStrategy.EARLIEST)

            forwardOnce(6, Duration.ofDays(1))

            monitoring {
                meters = true
            }
        }

        assertThat(scenario.rootSteps[0]).isInstanceOf(KafkaConsumerStepSpecification::class).all {
            prop(KafkaConsumerStepSpecification<*, *>::configuration).all {
                prop(KafkaConsumerConfiguration<*, *>::bootstrap).isEqualTo("my-bootstrap")
                prop(KafkaConsumerConfiguration<*, *>::concurrency).isEqualTo(2)
                prop(KafkaConsumerConfiguration<*, *>::topics).containsOnly("topic-1", "topic-2")
                prop(KafkaConsumerConfiguration<*, *>::topicsPattern).isNull()
                prop(KafkaConsumerConfiguration<*, *>::pollTimeout).isEqualTo(Duration.ofMillis(3))
                prop(KafkaConsumerConfiguration<*, *>::maxPolledRecords).isEqualTo(4)
                prop(KafkaConsumerConfiguration<*, *>::offsetReset).isEqualTo(
                    OffsetResetStrategy.EARLIEST
                )
                prop(KafkaConsumerConfiguration<*, *>::groupId).isEqualTo("my-group")
                prop(KafkaConsumerConfiguration<*, *>::flattenOutput).isFalse()
                prop(KafkaConsumerConfiguration<*, *>::properties).all {
                    key("key-1").isEqualTo("value-1")
                    key("key-2").isEqualTo("value-2")
                    key("key-3").isEqualTo(5)
                }
                prop(KafkaConsumerConfiguration<*, *>::keyDeserializer).isInstanceOf(ByteArrayDeserializer::class)
                prop(KafkaConsumerConfiguration<*, *>::valueDeserializer).isInstanceOf(ByteArrayDeserializer::class)
            }
            transform { it.monitoringConfig }.all {
                prop(StepMonitoringConfiguration::events).isFalse()
                prop(StepMonitoringConfiguration::meters).isTrue()
            }
            transform { it.singletonConfiguration }.all {
                prop(SingletonConfiguration::type).isEqualTo(SingletonType.UNICAST)
                prop(SingletonConfiguration::bufferSize).isEqualTo(6)
                prop(SingletonConfiguration::idleTimeout).isEqualTo(Duration.ofDays(1))
            }
        }
    }

    @Test
    internal fun `should apply topic pattern, poll timeout as duration and values bytes count`() {
        val scenario = scenario("my-scenario") as StepSpecificationRegistry
        scenario.kafka().consume {
            topics("topic-1", "topic-2")
            topicsPattern(Pattern.compile(".*"))  // This should clear the topics collection
            pollTimeout(Duration.ofMillis(3))

        }

        assertThat(scenario.rootSteps[0]).isInstanceOf(KafkaConsumerStepSpecification::class).all {
            prop(KafkaConsumerStepSpecification<*, *>::configuration).all {
                prop(KafkaConsumerConfiguration<*, *>::topics).hasSize(0)
                prop(KafkaConsumerConfiguration<*, *>::topicsPattern).transform { it!!.pattern() }
                    .isEqualTo(".*")
                prop(KafkaConsumerConfiguration<*, *>::pollTimeout).isEqualTo(Duration.ofMillis(3))
            }
        }
    }


    @Test
    internal fun `should flatten with other deserializer instances`() {
        val keyDeserializer = KeyDeserializer()
        val valueDeserializer = ValueDeserializer()

        val scenario = scenario("my-scenario") as StepSpecificationRegistry
        scenario.kafka().consume {}.flatten(keyDeserializer, valueDeserializer)

        assertThat(scenario.rootSteps[0]).isInstanceOf(KafkaConsumerStepSpecification::class).all {
            prop(KafkaConsumerStepSpecification<*, *>::configuration).all {
                prop(KafkaConsumerConfiguration<*, *>::keyDeserializer).isSameAs(keyDeserializer)
                prop(KafkaConsumerConfiguration<*, *>::valueDeserializer).isSameAs(valueDeserializer)
                prop(KafkaConsumerConfiguration<*, *>::flattenOutput).isTrue()
            }
        }
    }

    @Test
    internal fun `should flatten with other deserializer classes and keep the default configuration`() {
        val scenario = scenario("my-scenario") as StepSpecificationRegistry
        scenario.kafka().consume {}.flatten(KeyDeserializer::class, ValueDeserializer::class)

        assertThat(scenario.rootSteps[0]).isInstanceOf(KafkaConsumerStepSpecification::class).all {
            prop(KafkaConsumerStepSpecification<*, *>::configuration).all {
                prop(KafkaConsumerConfiguration<*, *>::keyDeserializer).isInstanceOf(
                    KeyDeserializer::class
                )
                prop(KafkaConsumerConfiguration<*, *>::valueDeserializer).isInstanceOf(
                    ValueDeserializer::class
                )
                prop(KafkaConsumerConfiguration<*, *>::flattenOutput).isTrue()
            }
        }
    }

    @Test
    internal fun `should flatten with other deserializer classes names and keep the default configuration`() {
        val scenario = scenario("my-scenario") as StepSpecificationRegistry
        scenario.kafka().consume {}
            .flatten<Int, String>(KeyDeserializer::class.jvmName, ValueDeserializer::class.jvmName)

        assertThat(scenario.rootSteps[0]).isInstanceOf(KafkaConsumerStepSpecification::class).all {
            prop(KafkaConsumerStepSpecification<*, *>::configuration).all {
                prop(KafkaConsumerConfiguration<*, *>::keyDeserializer).isInstanceOf(
                    KeyDeserializer::class
                )
                prop(KafkaConsumerConfiguration<*, *>::valueDeserializer).isInstanceOf(
                    ValueDeserializer::class
                )
                prop(KafkaConsumerConfiguration<*, *>::flattenOutput).isTrue()
            }
        }
    }


    @Test
    internal fun `should apply other deserializer instances`() {
        val keyDeserializer = KeyDeserializer()
        val valueDeserializer = ValueDeserializer()

        val scenario = scenario("my-scenario") as StepSpecificationRegistry
        scenario.kafka().consume {}.deserialize(keyDeserializer, valueDeserializer)

        assertThat(scenario.rootSteps[0]).isInstanceOf(KafkaConsumerStepSpecification::class).all {
            prop(KafkaConsumerStepSpecification<*, *>::configuration).all {
                prop(KafkaConsumerConfiguration<*, *>::keyDeserializer).isSameAs(keyDeserializer)
                prop(KafkaConsumerConfiguration<*, *>::valueDeserializer).isSameAs(valueDeserializer)
                prop(KafkaConsumerConfiguration<*, *>::flattenOutput).isFalse()
            }
        }
    }

    @Test
    internal fun `should apply other deserializer classes and keep the default configuration`() {
        val scenario = scenario("my-scenario") as StepSpecificationRegistry
        scenario.kafka().consume {}.deserialize(KeyDeserializer::class, ValueDeserializer::class)

        assertThat(scenario.rootSteps[0]).isInstanceOf(KafkaConsumerStepSpecification::class).all {
            prop(KafkaConsumerStepSpecification<*, *>::configuration).all {
                prop(KafkaConsumerConfiguration<*, *>::keyDeserializer).isInstanceOf(
                    KeyDeserializer::class
                )
                prop(KafkaConsumerConfiguration<*, *>::valueDeserializer).isInstanceOf(
                    ValueDeserializer::class
                )
                prop(KafkaConsumerConfiguration<*, *>::flattenOutput).isFalse()
            }
        }
    }

    @Test
    internal fun `should apply other deserializer classes names and keep the default configuration`() {
        val scenario = scenario("my-scenario") as StepSpecificationRegistry
        scenario.kafka().consume {}
            .deserialize<Int, String>(KeyDeserializer::class.jvmName, ValueDeserializer::class.jvmName)

        assertThat(scenario.rootSteps[0]).isInstanceOf(KafkaConsumerStepSpecification::class).all {
            prop(KafkaConsumerStepSpecification<*, *>::configuration).all {
                prop(KafkaConsumerConfiguration<*, *>::keyDeserializer).isInstanceOf(
                    KeyDeserializer::class
                )
                prop(KafkaConsumerConfiguration<*, *>::valueDeserializer).isInstanceOf(
                    ValueDeserializer::class
                )
                prop(KafkaConsumerConfiguration<*, *>::flattenOutput).isFalse()
            }
        }
    }

    class KeyDeserializer : Deserializer<Int> {
        override fun deserialize(topic: String, headers: Headers, data: ByteArray?): Int {
            return 1
        }

        override fun deserialize(topic: String?, data: ByteArray?): Int {
            throw NotImplementedError()
        }
    }

    class ValueDeserializer : Deserializer<String> {
        override fun deserialize(topic: String, headers: Headers, data: ByteArray?): String {
            return ""
        }

        override fun deserialize(topic: String?, data: ByteArray?): String {
            throw NotImplementedError()
        }
    }
}