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

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(
    JsonSubTypes.Type(value = Gauge::class, name = "gauge"),
    JsonSubTypes.Type(value = Counter::class, name = "counter"),
    JsonSubTypes.Type(value = Timer::class, name = "timer")
)
abstract class Meter {
    var timestamp: Instant? = null
    var name: String? = null

    @JsonAnySetter
    @JsonAnyGetter
    val tags: MutableMap<String, Any> = mutableMapOf()
}

class Gauge : Meter() {
    var value: Double? = null
    override fun toString(): String {
        return "Gauge(name=$name, tags=$tags, timestamp=$timestamp, value=$value)"
    }


}

class Counter : Meter() {
    var count: Int? = null
    override fun toString(): String {
        return "Counter(name=$name, tags=$tags, timestamp=$timestamp, count=$count)"
    }

}

class Timer : Meter() {
    var count: Int? = null
    var sum: Double? = null
    var mean: Double? = null
    var max: Double? = null
    override fun toString(): String {
        return "Timer(name=$name, tags=$tags, timestamp=$timestamp, count=$count, sum=$sum, mean=$mean, max=$max)"
    }

}