/*
 * Copyright 2024 AERIS IT Solutions GmbH
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
import assertk.assertions.isNotNull
import assertk.assertions.key
import assertk.assertions.prop
import io.micronaut.context.ApplicationContext
import io.micronaut.core.util.StringUtils
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import io.qalipsis.api.meters.MeasurementPublisherFactory
import io.qalipsis.plugins.kafka.config.KafkaMeasurementPublisherFactory
import io.qalipsis.test.assertk.typedProp
import jakarta.inject.Inject
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout

internal class KafkaMeasurementPublisherConfigIntegrationTest {

    @Nested
    @MicronautTest(environments = ["kafka"], startApplication = false)
    inner class WithoutRegistry {

        @Inject
        private lateinit var applicationContext: ApplicationContext

        @Test
        @Timeout(10)
        fun `should start without the registry`() {
            assertThat(applicationContext.getBeansOfType(KafkaMeasurementPublisher::class.java)).isEmpty()
            assertThat(applicationContext.getBeansOfType(MeasurementPublisherFactory::class.java)).isEmpty()
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
            assertThat(applicationContext.getBeansOfType(MeasurementPublisherFactory::class.java)).any {
                it.isInstanceOf(KafkaMeasurementPublisherFactory::class)
                    .prop(KafkaMeasurementPublisherFactory::getPublisher).isNotNull()
            }

            assertThat(applicationContext.getBean(KafkaMeasurementPublisherFactory::class.java).getPublisher()).all {
                isInstanceOf(KafkaMeasurementPublisher::class)
                    .typedProp<KafkaMeterConfig>("config").all {
                        prop(KafkaMeterConfig::prefix).isEqualTo("qalipsis")
                        prop(KafkaMeterConfig::topic).isEqualTo("qalipsis-meters")
                        prop(KafkaMeterConfig::timestampFieldName).isEqualTo("timestamp")
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
    }

    @Nested
    @MicronautTest(environments = ["kafka"], startApplication = false)
    inner class WithRegistry : TestPropertyProvider {

        @Inject
        private lateinit var applicationContext: ApplicationContext

        @Test
        @Timeout(10)
        internal fun `should start with the registry`() {
            assertThat(applicationContext.getBeansOfType(MeasurementPublisherFactory::class.java)).any {
                it.isInstanceOf(KafkaMeasurementPublisherFactory::class)
                    .prop(KafkaMeasurementPublisherFactory::getPublisher).all {
                        isInstanceOf(KafkaMeasurementPublisher::class.java)
                    }
            }

            assertThat(applicationContext.getBean(KafkaMeasurementPublisherFactory::class.java).getPublisher()).all {
                isInstanceOf(KafkaMeasurementPublisher::class)
                    .typedProp<KafkaMeterConfig>("config")
                    .all {
                        prop(KafkaMeterConfig::prefix).isEqualTo("qalipsis")
                        prop(KafkaMeterConfig::topic).isEqualTo("qalipsis-meters")
                        prop(KafkaMeterConfig::timestampFieldName).isEqualTo("timestamp")
                        prop(KafkaMeterConfig::configuration).all {
                            hasSize(4)
                            key("batch.size").isEqualTo(800000)
                            key("bootstrap.servers").isEqualTo("localhost:9092")
                            key("linger.ms").isEqualTo("5000")
                            key("delivery.timeout.ms").isEqualTo("10000")
                        }
                    }
            }
        }

        override fun getProperties(): Map<String, String> {
            return mapOf(
                "meters.export.kafka.enabled" to StringUtils.TRUE,
                "meters.export.enabled" to StringUtils.TRUE,
            )
        }
    }
}