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

package io.qalipsis.plugins.kafka.consumer

import assertk.all
import assertk.assertThat
import assertk.assertions.containsOnly
import assertk.assertions.hasSize
import io.qalipsis.plugins.kafka.Constants
import io.qalipsis.runtime.test.QalipsisTestRunner
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serdes
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.util.Properties

/**
 *
 * @author Eric Jessé
 */
@Testcontainers
internal class KafkaConsumerScenarioIntegrationTest {

    private lateinit var bootstrap: String

    private lateinit var kafkaProducer: KafkaProducer<Int, String>

    private lateinit var adminClient: AdminClient

    private var initialized = false

    @BeforeEach
    internal fun setUp() {
        if (!initialized) {
            bootstrap = container.bootstrapServers.substring("PLAINTEXT://".length)

            adminClient = AdminClient.create(Properties().also {
                it[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrap
                it[AdminClientConfig.CLIENT_ID_CONFIG] = "admin-client"
            })
            adminClient.createTopics(
                    listOf(
                            NewTopic(KafkaConsumerScenario.topicLeft, 1, 1),
                            NewTopic(KafkaConsumerScenario.topicRight, 1, 1)
                    )
            ).all().get()
            // Wait until the topics are actually created.
            while (adminClient.listTopics().names().get().size < 2) {
                Thread.sleep(500)
            }

            kafkaProducer = KafkaProducer(Properties().also {
                it[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrap
                it[ProducerConfig.COMPRESSION_TYPE_CONFIG] = "lz4"
                it[ProducerConfig.LINGER_MS_CONFIG] = 50
                it[ProducerConfig.CLIENT_ID_CONFIG] = "test-client"
            }, Serdes.Integer().serializer(), Serdes.String().serializer())

            initialized = true
        }
    }

    @Test
    @Timeout(20)
    internal fun `should run the consumer scenario`() {
        val generatedRightsRecordsKeys = (1..(2 * KafkaConsumerScenario.minions))
        generatedRightsRecordsKeys.forEach {
            kafkaProducer.send(ProducerRecord(KafkaConsumerScenario.topicRight, it, "Right #$it"))
        }

        // On the left side, only generate one every 2 records,
        val generatedLeftRecordsKeys = (1..(2 * KafkaConsumerScenario.minions)).filter { it % 2 == 0 }
        generatedLeftRecordsKeys.forEach {
            kafkaProducer.send(ProducerRecord(KafkaConsumerScenario.topicLeft, it, "Left #$it"))
        }

        KafkaConsumerScenario.bootstrap = bootstrap
        val exitCode = QalipsisTestRunner.withScenarios("consumer-kafka").execute()

        Assertions.assertEquals(0, exitCode)
        assertThat(KafkaConsumerScenario.receivedMessages).all {
            hasSize(KafkaConsumerScenario.minions)
            containsOnly(*generatedLeftRecordsKeys.map { "Left #$it - Right #$it" }.toTypedArray())
        }
    }

    companion object {

        @Container
        @JvmStatic
        private val container = KafkaContainer(DockerImageName.parse(Constants.DOCKER_IMAGE)).apply {
            withCreateContainerCmdModifier { cmd ->
                cmd.hostConfig!!.withMemory(Constants.DOCKER_MAX_MEMORY).withCpuCount(Constants.DOCKER_CPU_COUNT.toLong())
            }
            withEnv(Constants.KAFKA_HEAP_OPTS_ENV, Constants.KAFKA_HEAP_OPTS_ENV_VALUE)
        }

    }
}
