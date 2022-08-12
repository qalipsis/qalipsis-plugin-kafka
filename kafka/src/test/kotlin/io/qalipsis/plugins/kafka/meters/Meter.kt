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