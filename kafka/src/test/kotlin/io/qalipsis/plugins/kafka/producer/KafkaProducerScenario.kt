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

    @Scenario("producer-test-kafka")
    fun produceRecords() {
        scenario {
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