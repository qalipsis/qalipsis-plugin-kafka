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

package io.qalipsis.plugins.kafka.events

import io.micronaut.context.annotation.ConfigurationProperties
import io.micronaut.context.annotation.Requires
import io.qalipsis.api.constraints.PositiveOrZeroDuration
import io.qalipsis.api.events.EventLevel
import java.time.Duration
import java.util.*
import javax.validation.constraints.Min
import javax.validation.constraints.NotBlank
import javax.validation.constraints.NotNull

/**
 * Configuration for [KafkaEventsPublisher].
 *
 * @property minLevel minimal accepted level of events defaults to INFO.
 * @property bootstrap bootstraps of the Kafka broker, defaults to localhost:9092.
 * @property topic name of the topic to write the events to, default to qalipsis-events.
 * @property durationAsNano converts the duration as nanoseconds defaults to false to convert them as milliseconds.
 * @property lingerPeriod maximal period between two batches sending to the brokers, defaults to 1 second.
 * @property batchSize maximal number of events buffered between two publications of events to Kafka defaults to 2000.
 * @property configuration configuration for the Kafka broker, defaults to broker.
 * @property format format of the exported data, defaults to JSON.
 *
 * @author Eric Jessé
 */
@Requires(property = "events.export.kafka.enabled", value = "true")
@ConfigurationProperties("events.export.kafka")
internal class KafkaEventsConfiguration {

    @field:NotNull
    var minLevel: EventLevel = EventLevel.INFO

    @field:NotBlank
    var bootstrap: String = "localhost:9092"

    @field:NotBlank
    var topic: String = "qalipsis-events"

    var durationAsNano: Boolean = false

    @field:PositiveOrZeroDuration
    var lingerPeriod: Duration = Duration.ofSeconds(1)

    @field:Min(1)
    var batchSize: Int = 2000

    var configuration: Properties = Properties()

    var format: Format = Format.JSON

    enum class Format {
        JSON
    }
}
