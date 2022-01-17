package io.qalipsis.plugins.kafka.config

import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.isEmpty
import assertk.assertions.isNotEmpty
import io.micrometer.core.instrument.MeterRegistry
import io.micronaut.context.ApplicationContext
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import io.qalipsis.plugins.kafka.Constants
import io.qalipsis.plugins.kafka.micrometer.KafkaMeterRegistry
import jakarta.inject.Inject
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import kotlin.math.pow

@Testcontainers
internal class KafkaMeterRegistryFactoryIntegrationTest {

    @Nested
    @MicronautTest(startApplication = false)
    inner class WithoutMeters : TestPropertyProvider {

        @Inject
        private lateinit var applicationContext: ApplicationContext

        override fun getProperties(): MutableMap<String, String> {
            return mutableMapOf(
                "meters.enabled" to "false",
                "meters.kafka.enabled" to "true",
                "meters.bootstrap.servers" to container.bootstrapServers
            )
        }

        @Test
        @Timeout(10)
        internal fun `should start without kafka meter registry`() {
            assertThat(applicationContext.getBeansOfType(MeterRegistry::class.java)).isNotEmpty()
            assertThat(applicationContext.getBeansOfType(KafkaMeterRegistry::class.java)).isEmpty()
        }
    }

    @Nested
    @MicronautTest(startApplication = false)
    inner class WithMetersButWithoutKafka : TestPropertyProvider {

        @Inject
        private lateinit var applicationContext: ApplicationContext

        override fun getProperties(): MutableMap<String, String> {
            return mutableMapOf(
                "meters.enabled" to "true",
                "meters.kafka.enabled" to "false",
                "meters.bootstrap.servers" to container.bootstrapServers
            )
        }

        @Test
        @Timeout(10)
        internal fun `should start without kafka meter registry`() {
            assertThat(applicationContext.getBeansOfType(MeterRegistry::class.java)).isNotEmpty()
            assertThat(applicationContext.getBeansOfType(KafkaMeterRegistry::class.java)).isEmpty()
        }
    }

    @Nested
    @MicronautTest(startApplication = false)
    inner class WithKafkaMeterRegistry : TestPropertyProvider {

        @Inject
        private lateinit var applicationContext: ApplicationContext

        override fun getProperties(): MutableMap<String, String> {
            return mutableMapOf(
                "meters.enabled" to "true",
                "meters.kafka.enabled" to "true",
                "meters.bootstrap.servers" to container.bootstrapServers
            )
        }

        @Test
        @Timeout(10)
        internal fun `should start with kafka meter registry`() {
            assertThat(applicationContext.getBeansOfType(MeterRegistry::class.java)).isNotEmpty()
            assertThat(applicationContext.getBeansOfType(KafkaMeterRegistry::class.java)).hasSize(1)
        }
    }

    companion object {

        @Container
        @JvmStatic
        private val container = KafkaContainer(DockerImageName.parse(Constants.DOCKER_IMAGE)).apply {
            withCreateContainerCmdModifier { cmd ->
                cmd.hostConfig!!.withMemory(512 * 1024.0.pow(2).toLong()).withCpuCount(2)
            }
            withEnv("KAFKA_HEAP_OPTS", "-Xms256m -Xmx256m")
        }
    }
}