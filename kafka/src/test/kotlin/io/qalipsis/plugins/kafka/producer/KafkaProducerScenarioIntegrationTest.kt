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

package io.qalipsis.plugins.kafka.producer

import assertk.all
import assertk.assertThat
import assertk.assertions.containsOnly
import assertk.assertions.hasSize
import io.qalipsis.plugins.kafka.Constants
import io.qalipsis.plugins.kafka.Constants.DOCKER_CPU_COUNT
import io.qalipsis.plugins.kafka.Constants.DOCKER_MAX_MEMORY
import io.qalipsis.plugins.kafka.Constants.KAFKA_HEAP_OPTS_ENV_VALUE
import io.qalipsis.runtime.test.QalipsisTestRunner
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serdes
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.util.Properties

/**
 * @author Gabriel Moraes
 */
@Testcontainers
internal class KafkaProducerScenarioIntegrationTest {

    private lateinit var bootstrap: String

    private lateinit var kafkaProducer: KafkaProducer<Int, String>

    private lateinit var kafkaConsumer: KafkaConsumer<Int, String>

    private lateinit var adminClient: AdminClient

    @BeforeAll
    internal fun setUp() {
        bootstrap = container.bootstrapServers.substring("PLAINTEXT://".length)

        adminClient = AdminClient.create(Properties().also {
            it[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrap
            it[AdminClientConfig.CLIENT_ID_CONFIG] = "admin-client"
        })
        adminClient.createTopics(
            listOf(
                NewTopic(KafkaProducerScenario.testProducerTopic, 1, 1),
                NewTopic(KafkaProducerScenario.producerTopic, 1, 1)
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


        kafkaConsumer = KafkaConsumer(Properties().also {
            it[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrap
            it[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = OffsetResetStrategy.EARLIEST.toString().lowercase()
            it[ConsumerConfig.GROUP_ID_CONFIG] = "test-group"
        }, Serdes.Integer().deserializer(), Serdes.String().deserializer())

    }

    @Test
    @Timeout(20)
    internal fun `should run the producer scenario`() {

        val generatedLeftRecordsKeys = (1..(2 * KafkaProducerScenario.minions)).filter { it % 2 == 0 }
        generatedLeftRecordsKeys.forEach {
            kafkaProducer.send(ProducerRecord(KafkaProducerScenario.testProducerTopic, it, "Left #$it"))
        }

        KafkaProducerScenario.bootstrap = bootstrap
        val exitCode = QalipsisTestRunner.withScenarios("producer-test-kafka").execute()

        kafkaConsumer.subscribe(listOf(KafkaProducerScenario.producerTopic))
        val records = mutableSetOf<String>()

        while (records.size != generatedLeftRecordsKeys.size) {
            val consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100))
            if (!consumerRecords.isEmpty) {
                consumerRecords.map { records.add(it.value()) }
                kafkaConsumer.commitSync()
            }
        }

        assertThat(records).all {
            hasSize(KafkaProducerScenario.minions)
            containsOnly(*generatedLeftRecordsKeys.map { "Produced Left #$it" }.toTypedArray())
        }
        Assertions.assertEquals(0, exitCode)

    }

    companion object {

        @Container
        @JvmStatic
        private val container = KafkaContainer(DockerImageName.parse(Constants.DOCKER_IMAGE)).apply {
            withCreateContainerCmdModifier { cmd ->
                cmd.hostConfig!!.withMemory(DOCKER_MAX_MEMORY).withCpuCount(DOCKER_CPU_COUNT.toLong())
            }
            withEnv(Constants.KAFKA_HEAP_OPTS_ENV, KAFKA_HEAP_OPTS_ENV_VALUE)
        }

    }
}