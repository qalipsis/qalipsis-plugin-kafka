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

package io.qalipsis.plugins.kafka.config

import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.isEmpty
import assertk.assertions.isNotEmpty
import io.micronaut.context.ApplicationContext
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import io.qalipsis.api.meters.CampaignMeterRegistry
import io.qalipsis.plugins.kafka.meters.KafkaMeasurementPublisher
import jakarta.inject.Inject
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout

internal class KafkaMeasurementPublisherFactoryIntegrationTest {

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
            assertThat(applicationContext.getBeansOfType(KafkaMeasurementPublisherFactory::class.java)).isEmpty()
            assertThat(applicationContext.getBeansOfType(KafkaMeasurementPublisher::class.java)).isEmpty()
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
            assertThat(applicationContext.getBeansOfType(KafkaMeasurementPublisherFactory::class.java)).isEmpty()
            assertThat(applicationContext.getBeansOfType(KafkaMeasurementPublisher::class.java)).isEmpty()
        }
    }

    @Nested
    @MicronautTest(startApplication = false, environments = ["kafka"])
    inner class WithKafkaMeasurementPublisher : TestPropertyProvider {

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
            assertThat(applicationContext.getBeansOfType(CampaignMeterRegistry::class.java)).isNotEmpty()
            assertThat(applicationContext.getBeansOfType(KafkaMeasurementPublisherFactory::class.java)).hasSize(1)
        }
    }

}