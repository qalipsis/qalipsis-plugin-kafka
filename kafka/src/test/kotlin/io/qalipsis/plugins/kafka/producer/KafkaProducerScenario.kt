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

import io.qalipsis.api.annotations.Scenario
import io.qalipsis.api.executionprofile.regular
import io.qalipsis.api.scenario.scenario
import io.qalipsis.api.steps.blackHole
import io.qalipsis.plugins.kafka.consumer.consume
import io.qalipsis.plugins.kafka.kafka
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.serialization.Serdes

/**
 *
 * @author Gabriel Moraes
 */
internal object KafkaProducerScenario {

    const val minions = 20

    const val testProducerTopic = "test.topic"

    const val producerTopic = "producer.topic"

    internal var bootstrap: String = ""

    private val intDeserializer = Serdes.Integer().deserializer()

    private val stringDeserializer = Serdes.String().deserializer()

    @Scenario
    fun produceRecords() {
        scenario("producer-test-kafka") {
            minionsCount = minions
            profile {
                // Starts all at once.
                regular(100, minionsCount)
            }
        }
            .start()
            .kafka()
            .consume {
                name = "kafka-consumer"
                bootstrap(bootstrap)
                topics(testProducerTopic)
                groupId("kafka-left")
                pollTimeout(100)
                offsetReset(OffsetResetStrategy.EARLIEST)
            }.flatten(intDeserializer, stringDeserializer)
            .kafka().produce(
                Serdes.ByteArray().serializer(),
                Serdes.String().serializer()
            ) {
                name = "kafka-producer"
                bootstrap(bootstrap)
                clientName("producer")
                records { _, input ->
                    listOf(
                        KafkaProducerRecord(
                            topic = producerTopic,
                            value = "Produced ${input.record.value!!}"
                        )
                    )
                }
            }
            .blackHole()
    }
}