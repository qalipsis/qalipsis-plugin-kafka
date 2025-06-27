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
            MeterType.RATE -> writeRate(meterSnapshot)
            MeterType.THROUGHPUT -> writeThroughput(meterSnapshot)
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
                write(counterSnapshot) { }
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
                write(gaugeSnapshot) { }
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
     * Kafka serializer for Rate.
     */
    private fun writeRate(rateSnapshot: MeterSnapshot): String {
        return rateSnapshot.measurements.joinToString(",") {
            val value = it.value
            if (java.lang.Double.isFinite(value)) {
                write(rateSnapshot) { builder: StringBuilder -> builder.append(",\"value\":").append(value) }
            } else {
                write(rateSnapshot) { }
            }
        }
    }

    /**
     * Kafka serializer for Throughput.
     */
    private fun writeThroughput(throughputSnapshot: MeterSnapshot): String {
        val intermediaryString = StringBuilder()
        throughputSnapshot.measurements.forEach {
            when (it.statistic) {
                Statistic.VALUE -> intermediaryString.append(",\"value\":").append(it.value)
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

        return write(throughputSnapshot) { builder: StringBuilder -> builder.append(intermediaryString) }
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
