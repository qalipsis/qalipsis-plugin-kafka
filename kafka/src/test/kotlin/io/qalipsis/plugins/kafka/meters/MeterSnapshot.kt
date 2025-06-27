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

package io.qalipsis.plugins.kafka.meters

import com.fasterxml.jackson.annotation.JsonAnyGetter
import com.fasterxml.jackson.annotation.JsonAnySetter
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import java.time.Instant
import java.util.concurrent.TimeUnit

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(
    JsonSubTypes.Type(value = GaugeSnapshot::class, name = "gauge"),
    JsonSubTypes.Type(value = CounterSnapshot::class, name = "counter"),
    JsonSubTypes.Type(value = TimerSnapshot::class, name = "timer"),
    JsonSubTypes.Type(value = DistributionSummarySnapshot::class, name = "summary"),
    JsonSubTypes.Type(value = RateSnapshot::class, name = "rate"),
    JsonSubTypes.Type(value = ThroughputSnapshot::class, name = "throughput")
)
abstract class MeterSnapshot(
    val timestamp: Instant,
    val name: String,
    val type: String,
    val tags: Map<String, String>? = null
)

class GaugeSnapshot(
    timestamp: Instant,
    name: String,
    tags: Map<String, String>? = null,
    val value: Double
) : MeterSnapshot(timestamp, name, "gauge", tags) {

    override fun toString(): String {
        return "Gauge(name=$name, type=$type, tags=$tags, timestamp=$timestamp, value=$value)"
    }
}

class CounterSnapshot(
    timestamp: Instant,
    name: String,
    tags: Map<String, String>? = null,
    val count: Int
) : MeterSnapshot(timestamp, name, "counter", tags) {

    override fun toString(): String {
        return "Counter(name=$name,  type=$type, tags=$tags, timestamp=$timestamp, count=$count)"
    }
}

class RateSnapshot(
    timestamp: Instant,
    name: String,
    tags: Map<String, String>? = null,
    val value: Double
) : MeterSnapshot(timestamp, name, "rate", tags) {

    override fun toString(): String {
        return "Rate(name=$name,  type=$type, tags=$tags, timestamp=$timestamp, value=$value)"
    }
}

class TimerSnapshot(
    timestamp: Instant,
    name: String,
    tags: Map<String, String>? = null,
    var count: Int,
    var sum: Double,
    var mean: Double,
    var max: Double,
    var unit: TimeUnit,
    @JsonAnySetter
    @JsonAnyGetter
    val others: MutableMap<String, Any> = mutableMapOf()
) : MeterSnapshot(timestamp, name, "timer", tags) {

    override fun toString(): String {
        return "Timer(name=$name, type=$type, tags=$tags, timestamp=$timestamp, count=$count, sum=$sum, mean=$mean, max=$max, others=$others)"
    }
}

class DistributionSummarySnapshot(
    timestamp: Instant,
    name: String,
    tags: Map<String, String>? = null,
    var count: Int,
    var sum: Double,
    var mean: Double,
    var max: Double,
    @JsonAnySetter
    @JsonAnyGetter
    val others: MutableMap<String, Any> = mutableMapOf()
) : MeterSnapshot(timestamp, name, "summary", tags) {

    override fun toString(): String {
        return "DistributionSummary(name=$name, type=$type, tags=$tags, timestamp=$timestamp, count=$count, sum=$sum, mean=$mean, max=$max, others=$others)"
    }
}

class ThroughputSnapshot(
    timestamp: Instant,
    name: String,
    tags: Map<String, String>? = null,
    var value: Double,
    var sum: Double,
    var mean: Double,
    var max: Double,
    @JsonAnySetter
    @JsonAnyGetter
    val others: MutableMap<String, Any> = mutableMapOf()
) : MeterSnapshot(timestamp, name, "throughput", tags) {

    override fun toString(): String {
        return "Throughput(name=$name, type=$type, tags=$tags, timestamp=$timestamp, value=$value, sum=$sum, mean=$mean, max=$max, others=$others)"
    }
}