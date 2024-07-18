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
    JsonSubTypes.Type(value = DistributionSummarySnapshot::class, name = "summary")
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