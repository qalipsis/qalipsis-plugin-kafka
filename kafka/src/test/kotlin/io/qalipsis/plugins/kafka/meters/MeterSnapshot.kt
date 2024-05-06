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

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "@type")
@JsonSubTypes(
    JsonSubTypes.Type(value = GaugeSnapshot::class, name = "gauge"),
    JsonSubTypes.Type(value = CounterSnapshot::class, name = "counter"),
    JsonSubTypes.Type(value = TimerSnapshot::class, name = "timer"),
    JsonSubTypes.Type(value = DistributionSummarySnapshot::class, name = "summary")
)
abstract class MeterSnapshot {
    var timestamp: Instant? = null
    var name: String? = null
    var type: String? = null

    @JsonAnySetter
    @JsonAnyGetter
    val others: MutableMap<String, Any> = mutableMapOf()
}

class GaugeSnapshot : MeterSnapshot() {
    var value: Double? = null
    override fun toString(): String {
        return "Gauge(name=$name, type=$type, others=$others, timestamp=$timestamp, value=$value)"
    }


}

class CounterSnapshot : MeterSnapshot() {
    var count: Int? = null
    override fun toString(): String {
        return "Counter(name=$name,  type=$type, others=$others, timestamp=$timestamp, count=$count)"
    }

}

class TimerSnapshot : MeterSnapshot() {
    var sum: Double? = null
    var mean: Double? = null
    var max: Double? = null
    override fun toString(): String {
        return "Timer(name=$name, type=$type, others=$others, timestamp=$timestamp, sum=$sum, mean=$mean, max=$max)"
    }

}

class DistributionSummarySnapshot : MeterSnapshot() {
    var sum: Double? = null
    var mean: Double? = null
    var max: Double? = null
    override fun toString(): String {
        return "DistributionSummary(name=$name, type=$type, others=$others, timestamp=$timestamp, sum=$sum, mean=$mean, max=$max)"
    }

}