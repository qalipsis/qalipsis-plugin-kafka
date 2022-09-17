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

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.FunctionCounter
import io.micrometer.core.instrument.FunctionTimer
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.LongTaskTimer
import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.TimeGauge
import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.util.StringEscapeUtils
import org.apache.kafka.common.serialization.Serializer
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.Locale
import java.util.concurrent.TimeUnit
import java.util.function.Consumer

/**
 * Kafka serializer for different kind of Мeters as JSON.
 *
 * Author: Palina Bril
 */
internal class JsonMeterSerializer(
    private val meterRegistry: KafkaMeterRegistry,
    private val timeUnit: TimeUnit,
    private val timestampFieldName: String,
) : Serializer<Meter> {

    override fun serialize(topic: String, meter: Meter): ByteArray? {
        val data = when (meter) {
            is TimeGauge -> writeTimeGauge(meter)
            is Gauge -> writeGauge(meter)
            is Counter -> writeCounter(meter)
            is Timer -> writeTimer(meter)
            is DistributionSummary -> writeSummary(meter)
            is LongTaskTimer -> writeLongTaskTimer(meter)
            is FunctionCounter -> writeFunctionCounter(meter)
            is FunctionTimer -> writeFunctionTimer(meter)
            else -> writeMeter(meter)
        }
        return data?.encodeToByteArray()
    }

    /**
     * Kafka serializer for Counter.
     */
    private fun writeCounter(counter: Counter): String {
        return writeCounter(counter, counter.count())
    }

    /**
     * Kafka serializer for FunctionCounter.
     */
    private fun writeFunctionCounter(counter: FunctionCounter): String {
        return writeCounter(counter, counter.count())
    }

    /**
     * Kafka serializer for Counter with value.
     */
    private fun writeCounter(meter: Meter, value: Double): String {
        return if (java.lang.Double.isFinite(value)) {
            write(meter) { builder: StringBuilder -> builder.append(",\"count\":").append(value) }
        } else write(meter) { builder: StringBuilder -> builder }
    }

    /**
     * Kafka serializer for Gauge.
     */
    private fun writeGauge(gauge: Gauge): String {
        val value = gauge.value()
        return if (java.lang.Double.isFinite(value)) {
            write(gauge) { builder: StringBuilder -> builder.append(",\"value\":").append(value) }
        } else write(gauge) { builder: StringBuilder -> builder }
    }

    /**
     * Kafka serializer for TimeGauge.
     */
    private fun writeTimeGauge(gauge: TimeGauge): String {
        val value = gauge.value(timeUnit)
        return if (java.lang.Double.isFinite(value)) {
            write(gauge) { builder: StringBuilder -> builder.append(",\"value\":").append(value) }
        } else write(gauge) { builder: StringBuilder -> builder }
    }

    /**
     * Kafka serializer for FunctionTimer.
     */
    private fun writeFunctionTimer(timer: FunctionTimer): String {
        val sum = timer.totalTime(timeUnit)
        val mean = timer.mean(timeUnit)
        return write(timer) { builder: StringBuilder ->
            builder.append(",\"count\":").append(timer.count())
            builder.append(",\"sum\":").append(sum)
            builder.append(",\"mean\":").append(mean)
        }
    }

    /**
     * Kafka serializer for LongTaskTimer.
     */
    private fun writeLongTaskTimer(timer: LongTaskTimer): String {
        return write(timer) { builder: StringBuilder ->
            builder.append(",\"activeTasks\":").append(timer.activeTasks())
            builder.append(",\"duration\":").append(timer.duration(timeUnit))
        }
    }

    /**
     * Kafka serializer for Timer.
     */
    private fun writeTimer(timer: Timer): String {
        return write(timer) { builder: StringBuilder ->
            builder.append(",\"count\":").append(timer.count())
            builder.append(",\"sum\":").append(timer.totalTime(timeUnit))
            builder.append(",\"mean\":").append(timer.mean(timeUnit))
            builder.append(",\"max\":").append(timer.max(timeUnit))
        }
    }

    /**
     * Kafka serializer for DistributionSummary.
     */
    private fun writeSummary(summary: DistributionSummary): String {
        val histogramSnapshot = summary.takeSnapshot()
        return write(summary) { builder: StringBuilder ->
            builder.append(",\"count\":").append(histogramSnapshot.count())
            builder.append(",\"sum\":").append(histogramSnapshot.total())
            builder.append(",\"mean\":").append(histogramSnapshot.mean())
            builder.append(",\"max\":").append(histogramSnapshot.max())
        }
    }

    /**
     * Kafka further serializer for previous kinds of Meter
     */
    private fun writeMeter(meter: Meter): String? {
        val measurements = meter.measure()
        val names = mutableListOf<String>()
        // Snapshot values should be used throughout this method as there are chances for values to be changed in-between.
        val values = mutableListOf<Double>()
        for (measurement in measurements) {
            val value = measurement.value
            if (!java.lang.Double.isFinite(value)) {
                continue
            }
            names.add(measurement.statistic.tagValueRepresentation)
            values.add(value)
        }
        return if (names.isEmpty()) {
            null
        } else {
            write(meter) { builder: StringBuilder ->
                for (i in names.indices) builder.append(",\"").append(names[i])
                    .append("\":\"").append(values[i]).append("\"")
            }
        }
    }

    /**
     * Kafka final serializer for all kinds of Meter
     */
    private fun write(meter: Meter, consumer: Consumer<StringBuilder>): String {
        val sb: StringBuilder = StringBuilder("")
        val timestamp = generateTimestamp()
        val name = meterRegistry.getName(meter)
        val type = meter.id.type.toString().lowercase(Locale.getDefault())
        sb.append("{\"").append(timestampFieldName).append("\":\"").append(timestamp).append('"')
            .append(",\"name\":\"").append(StringEscapeUtils.escapeJson(name)).append('"')
            .append(",\"type\":\"").append(type).append('"')
        val tags = meterRegistry.getTags(meter)
        for (tag in tags) {
            sb.append(",\"").append(StringEscapeUtils.escapeJson(tag.key)).append("\":\"")
                .append(StringEscapeUtils.escapeJson(tag.value)).append('"')
        }
        consumer.accept(sb)
        sb.append('}')
        return sb.toString()
    }

    private fun generateTimestamp(): String? {
        return DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(meterRegistry.config().clock().wallTime()))
    }
}
