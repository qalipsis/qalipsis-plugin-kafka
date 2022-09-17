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
import assertk.assertions.hasSize
import assertk.assertions.isEqualTo
import assertk.assertions.key
import assertk.assertions.prop
import io.micronaut.context.ApplicationContext
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.qalipsis.plugins.kafka.meters.KafkaMeterRegistry
import io.qalipsis.test.assertk.typedProp
import jakarta.inject.Inject
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout

@MicronautTest(environments = ["kafka", "kafka-config-test"])
internal class KafkaMeterRegistryConfigIntegrationTest {

    @Inject
    private lateinit var applicationContext: ApplicationContext

    @Test
    @Timeout(10)
    internal fun `should start with the registry`() {
        assertThat(applicationContext.getBean(KafkaMeterRegistry::class.java))
            .typedProp<KafkaMeterConfig>("config").all {
                prop(KafkaMeterConfig::prefix).isEqualTo("kafka")
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