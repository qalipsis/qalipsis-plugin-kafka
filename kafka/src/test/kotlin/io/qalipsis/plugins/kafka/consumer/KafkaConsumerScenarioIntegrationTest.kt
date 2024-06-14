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
import java.util.*

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
