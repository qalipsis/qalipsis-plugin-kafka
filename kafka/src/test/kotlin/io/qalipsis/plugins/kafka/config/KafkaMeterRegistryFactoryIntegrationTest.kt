package io.qalipsis.plugins.kafka.config

import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.isEmpty
import assertk.assertions.isNotEmpty
import io.micrometer.core.instrument.MeterRegistry
import io.micronaut.context.ApplicationContext
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import io.qalipsis.plugins.kafka.meters.KafkaMeterRegistry
import jakarta.inject.Inject
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout

internal class KafkaMeterRegistryFactoryIntegrationTest {

    @Nested
    @MicronautTest(startApplication = false, environments = ["kafka"])
    inner class WithoutMeters : TestPropertyProvider {

        @Inject
        private lateinit var applicationContext: ApplicationContext

        override fun getProperties(): MutableMap<String, String> {
            return mutableMapOf(
                "meters.export.enabled" to "false",
                "meters.export.kafka.enabled" to "true"
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
    @MicronautTest(startApplication = false, environments = ["kafka"])
    inner class WithMetersButWithoutKafka : TestPropertyProvider {

        @Inject
        private lateinit var applicationContext: ApplicationContext

        override fun getProperties(): MutableMap<String, String> {
            return mutableMapOf(
                "meters.export.enabled" to "true",
                "meters.export.kafka.enabled" to "false"
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
    @MicronautTest(startApplication = false, environments = ["kafka"])
    inner class WithKafkaMeterRegistry : TestPropertyProvider {

        @Inject
        private lateinit var applicationContext: ApplicationContext

        override fun getProperties(): MutableMap<String, String> {
            return mutableMapOf(
                "meters.export.enabled" to "true",
                "meters.export.kafka.enabled" to "true"
            )
        }

        @Test
        @Timeout(10)
        internal fun `should start with kafka meter registry`() {
            assertThat(applicationContext.getBeansOfType(MeterRegistry::class.java)).isNotEmpty()
            assertThat(applicationContext.getBeansOfType(KafkaMeterRegistry::class.java)).hasSize(1)
        }
    }

}