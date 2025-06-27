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

import io.qalipsis.api.annotations.Scenario
import io.qalipsis.api.executionprofile.regular
import io.qalipsis.api.lang.concurrentSet
import io.qalipsis.api.scenario.scenario
import io.qalipsis.api.steps.blackHole
import io.qalipsis.api.steps.filterNotNull
import io.qalipsis.api.steps.innerJoin
import io.qalipsis.api.steps.map
import io.qalipsis.api.steps.onEach
import io.qalipsis.plugins.kafka.kafka
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.serialization.Serdes

/**
 *
 * @author Eric Jessé
 */
internal object KafkaConsumerScenario {

    const val minions = 20

    const val topicLeft = "left"

    const val topicRight = "right"

    val receivedMessages = concurrentSet<String>()

    internal var bootstrap: String = ""

    private val intDeserializer = Serdes.Integer().deserializer()

    private val stringDeserializer = Serdes.String().deserializer()

    @Scenario("consumer-kafka")
    fun consumeRecordsAndCorrelate() {
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
                name = "left-kafka-consumer"
                bootstrap(bootstrap)
                topics(topicLeft)
                groupId("kafka-left")
                pollTimeout(100)
                offsetReset(OffsetResetStrategy.EARLIEST)
            }.flatten(intDeserializer, stringDeserializer)
            .innerJoin()
            .using { it.value.record.key }
            .on {
                    it.kafka()
                        .consume {
                            name = "right-kafka-consumer"
                            bootstrap(bootstrap)
                            topics(topicRight)
                            groupId("kafka-right")
                            pollTimeout(100)
                            offsetReset(OffsetResetStrategy.EARLIEST)
                        }.flatten(intDeserializer, stringDeserializer)
            }
            .having { it.value.record.key }
            .map { joinResult -> joinResult?.first?.let { "${it.record.value} - ${joinResult.second.record.value}" } }
            .filterNotNull()
            .onEach { receivedMessages.add(it) }
            .blackHole()
    }
}