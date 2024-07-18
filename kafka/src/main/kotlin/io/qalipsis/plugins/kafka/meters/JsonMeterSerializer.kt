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

import io.qalipsis.api.meters.DistributionMeasurementMetric
import io.qalipsis.api.meters.MeterSnapshot
import io.qalipsis.api.meters.MeterType
import io.qalipsis.api.meters.Statistic
import io.qalipsis.api.meters.UnsupportedMeterException
import org.apache.commons.text.StringEscapeUtils
import org.apache.kafka.common.serialization.Serializer
import java.util.concurrent.TimeUnit
import java.util.function.Consumer

/**
 * Kafka serializer for different kind of Мeters as JSON.
 *
 * Author: Palina Bril
 */
internal class JsonMeterSerializer(
    private val timestampFieldName: String,
) : Serializer<MeterSnapshot> {

    override fun serialize(topic: String, meterSnapshot: MeterSnapshot): ByteArray {
        val data = when (meterSnapshot.meterId.type) {
            MeterType.GAUGE -> writeGauge(meterSnapshot)
            MeterType.COUNTER -> writeCounter(meterSnapshot)
            MeterType.TIMER -> writeTimer(meterSnapshot)
            MeterType.DISTRIBUTION_SUMMARY -> writeSummary(meterSnapshot)
            else -> throw UnsupportedMeterException("Meter ${meterSnapshot.meterId} not supported")
        }
        return data.encodeToByteArray()
    }

    /**
     * Kafka serializer for Counter with value.
     */
    private fun writeCounter(counterSnapshot: MeterSnapshot): String {
        return counterSnapshot.measurements.joinToString(",") {
            val value = it.value
            if (java.lang.Double.isFinite(value)) {
                write(counterSnapshot) { builder: StringBuilder -> builder.append(",\"count\":").append(value) }
            } else {
                write(counterSnapshot) { builder: StringBuilder -> builder }
            }
        }
    }

    /**
     * Kafka serializer for Gauge.
     */
    private fun writeGauge(gaugeSnapshot: MeterSnapshot): String {
        return gaugeSnapshot.measurements.joinToString(",") {
            val value = it.value
            if (java.lang.Double.isFinite(value)) {
                write(gaugeSnapshot) { builder: StringBuilder -> builder.append(",\"value\":").append(value) }
            } else {
                write(gaugeSnapshot) { builder: StringBuilder -> builder }
            }
        }
    }

    /**
     * Kafka serializer for Timer.
     */
    private fun writeTimer(timerSnapshot: MeterSnapshot): String {
        val intermediaryString = StringBuilder()
        intermediaryString.append(",\"unit\":").append("\"${TimeUnit.MICROSECONDS}\"")
        timerSnapshot.measurements.forEach {
            when (it.statistic) {
                Statistic.COUNT -> intermediaryString.append(",\"count\":").append(it.value)
                Statistic.TOTAL_TIME -> intermediaryString.append(",\"sum\":").append(it.value)
                Statistic.MEAN -> intermediaryString.append(",\"mean\":").append(it.value)
                Statistic.MAX -> intermediaryString.append(",\"max\":").append(it.value)
                Statistic.PERCENTILE -> {
                    it as DistributionMeasurementMetric
                    intermediaryString.append(",\"percentile_${it.observationPoint.toString().replace('.', '_')}\":")
                        .append(it.value)
                }

                else -> intermediaryString
            }
        }
        return write(timerSnapshot) { builder: StringBuilder -> builder.append(intermediaryString) }
    }

    /**
     * Kafka serializer for DistributionSummary.
     */
    private fun writeSummary(summarySnapshot: MeterSnapshot): String {
        val intermediaryString = StringBuilder()
        summarySnapshot.measurements.forEach {
            when (it.statistic) {
                Statistic.COUNT -> intermediaryString.append(",\"count\":").append(it.value)
                Statistic.TOTAL -> intermediaryString.append(",\"sum\":").append(it.value)
                Statistic.MEAN -> intermediaryString.append(",\"mean\":").append(it.value)
                Statistic.MAX -> intermediaryString.append(",\"max\":").append(it.value)
                Statistic.PERCENTILE -> {
                    it as DistributionMeasurementMetric
                    intermediaryString.append(",\"percentile_${it.observationPoint.toString().replace('.', '_')}\":")
                        .append(it.value)
                }

                else -> intermediaryString
            }
        }

        return write(summarySnapshot) { builder: StringBuilder -> builder.append(intermediaryString) }
    }

    /**
     * Kafka final serializer for all kinds of Measurements.
     */
    private fun write(meterSnapshot: MeterSnapshot, consumer: Consumer<StringBuilder>): String {
        val sb: StringBuilder = StringBuilder("")
        val meterId = meterSnapshot.meterId
        val name = meterId.meterName
        sb.append("{\"").append(timestampFieldName).append("\":\"").append(meterSnapshot.timestamp).append('"')
            .append(",\"name\":\"").append(StringEscapeUtils.escapeJson(name)).append('"')
            .append(",\"type\":\"").append(meterId.type.value).append('"')
        val tags = meterId.tags
        if (tags.isNotEmpty()) {
            sb.append(",\"tags\":{").append(tags.toList().joinToString { (key, value) ->
                """"${StringEscapeUtils.escapeJson(key)}":"${StringEscapeUtils.escapeJson(value)}""""
            })
            sb.append("}")
        }
        consumer.accept(sb)
        sb.append('}')
        return sb.toString()
    }
}
