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

package io.qalipsis.plugins.kafka.events

import io.micronaut.context.annotation.ConfigurationProperties
import io.micronaut.context.annotation.Requires
import io.qalipsis.api.constraints.PositiveOrZeroDuration
import io.qalipsis.api.events.EventLevel
import java.time.Duration
import java.util.Properties
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
