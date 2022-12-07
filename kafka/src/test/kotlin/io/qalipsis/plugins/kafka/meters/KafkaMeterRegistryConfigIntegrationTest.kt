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

import assertk.all
import assertk.assertThat
import assertk.assertions.any
import assertk.assertions.hasSize
import assertk.assertions.isEmpty
import assertk.assertions.isEqualTo
import assertk.assertions.isInstanceOf
import assertk.assertions.key
import assertk.assertions.prop
import io.micrometer.core.instrument.MeterRegistry
import io.micronaut.context.ApplicationContext
import io.micronaut.core.util.StringUtils
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import io.qalipsis.api.meters.MeterRegistryConfiguration
import io.qalipsis.api.meters.MeterRegistryFactory
import io.qalipsis.test.assertk.typedProp
import jakarta.inject.Inject
import java.time.Duration
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout

internal class KafkaMeterRegistryConfigIntegrationTest {

    @Nested
    @MicronautTest(environments = ["kafka"], startApplication = false)
    inner class WithoutRegistry {

        @Inject
        private lateinit var applicationContext: ApplicationContext

        @Test
        @Timeout(10)
        fun `should start without the registry`() {
            assertThat(applicationContext.getBeansOfType(KafkaMeterRegistry::class.java)).isEmpty()
            assertThat(applicationContext.getBeansOfType(MeterRegistryFactory::class.java)).isEmpty()
        }
    }

    @Nested
    @MicronautTest(environments = ["kafka-config-test"], startApplication = false)
    inner class WithConfiguredRegistry {

        @Inject
        private lateinit var applicationContext: ApplicationContext

        @Test
        @Timeout(10)
        internal fun `should start with the configured registry`() {
            assertThat(applicationContext.getBeansOfType(MeterRegistry::class.java)).any {
                it.isInstanceOf(KafkaMeterRegistry::class)
            }

            assertThat(applicationContext.getBean(KafkaMeterRegistry::class.java)).typedProp<KafkaMeterConfig>("config").all {
                prop(KafkaMeterConfig::prefix).isEqualTo("kafka")
                prop(KafkaMeterConfig::topic).isEqualTo("qalipsis-meters")
                prop(KafkaMeterConfig::timestampFieldName).isEqualTo("timestamp")
                prop(KafkaMeterConfig::step).isEqualTo(Duration.ofSeconds(10))
                prop(KafkaMeterConfig::configuration).all {
                    hasSize(4)
                    key("batch.size").isEqualTo(500)
                    key("bootstrap.servers").isEqualTo("kafka-1:9092")
                    key("linger.ms").isEqualTo("3000")
                    key("delivery.timeout.ms").isEqualTo("14000")
                }
            }
        }
    }

    @Nested
    @MicronautTest(environments = ["kafka"], startApplication = false)
    inner class WithRegistry : TestPropertyProvider{

        @Inject
        private lateinit var applicationContext: ApplicationContext

        @Test
        @Timeout(10)
        internal fun `should start with the registry`() {
            assertThat(applicationContext.getBeansOfType(MeterRegistry::class.java)).any {
                it.isInstanceOf(KafkaMeterRegistry::class)
            }

            assertThat(applicationContext.getBean(KafkaMeterRegistry::class.java)).typedProp<KafkaMeterConfig>("config").all {
                prop(KafkaMeterConfig::prefix).isEqualTo("kafka")
                prop(KafkaMeterConfig::topic).isEqualTo("qalipsis-meters")
                prop(KafkaMeterConfig::timestampFieldName).isEqualTo("timestamp")
                prop(KafkaMeterConfig::step).isEqualTo(Duration.ofSeconds(10))
                prop(KafkaMeterConfig::configuration).all {
                    hasSize(4)
                    key("batch.size").isEqualTo(800000)
                    key("bootstrap.servers").isEqualTo("localhost:9092")
                    key("linger.ms").isEqualTo("5000")
                    key("delivery.timeout.ms").isEqualTo("10000")
                }
            }

            val meterRegistryFactory = applicationContext.getBean(MeterRegistryFactory::class.java)
            var generatedMeterRegistry = meterRegistryFactory.getRegistry(
                object : MeterRegistryConfiguration {
                    override val step: Duration? = null

                }
            )
            assertThat(generatedMeterRegistry).typedProp<KafkaMeterConfig>("config").all {
                prop(KafkaMeterConfig::prefix).isEqualTo("kafka")
                prop(KafkaMeterConfig::topic).isEqualTo("qalipsis-meters")
                prop(KafkaMeterConfig::timestampFieldName).isEqualTo("timestamp")
                prop(KafkaMeterConfig::step).isEqualTo(Duration.ofSeconds(10))
                prop(KafkaMeterConfig::configuration).all {
                    hasSize(3)
                    key("batch.size").isEqualTo(10000)
                    key("bootstrap.servers").isEqualTo("localhost:9092")
                    key("linger.ms").isEqualTo("1000")
                }
            }

            generatedMeterRegistry = meterRegistryFactory.getRegistry(
                object : MeterRegistryConfiguration {
                    override val step: Duration = Duration.ofSeconds(3)

                }
            )
            assertThat(generatedMeterRegistry).typedProp<KafkaMeterConfig>("config").all {
                prop(KafkaMeterConfig::prefix).isEqualTo("kafka")
                prop(KafkaMeterConfig::topic).isEqualTo("qalipsis-meters")
                prop(KafkaMeterConfig::timestampFieldName).isEqualTo("timestamp")
                prop(KafkaMeterConfig::step).isEqualTo(Duration.ofSeconds(3))
                prop(KafkaMeterConfig::configuration).all {
                    hasSize(3)
                    key("batch.size").isEqualTo(10000)
                    key("bootstrap.servers").isEqualTo("localhost:9092")
                    key("linger.ms").isEqualTo("1000")
                }
            }
        }

        override fun getProperties(): Map<String, String> {
            return mapOf(
                "meters.export.kafka.enabled" to StringUtils.TRUE,
            )
        }
    }
}