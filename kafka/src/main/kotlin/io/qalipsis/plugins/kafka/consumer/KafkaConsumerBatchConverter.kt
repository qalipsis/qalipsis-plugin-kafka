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

package io.qalipsis.plugins.kafka.consumer

import io.qalipsis.api.context.StepOutput
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.lang.tryAndLogOrNull
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.meters.CampaignMeterRegistry
import io.qalipsis.api.meters.Counter
import io.qalipsis.api.report.ReportMessageSeverity
import io.qalipsis.api.steps.datasource.DatasourceObjectConverter
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.common.serialization.Deserializer
import java.util.concurrent.atomic.AtomicLong

/**
 *
 * Implementation of [DatasourceObjectConverter], that reads a batch of native Kafka records and forwards it
 * as a list of [KafkaConsumerRecord].
 *
 * @author Eric Jessé
 */
internal class KafkaConsumerBatchConverter<K, V>(
    private val keyDeserializer: Deserializer<K>,
    private val valueDeserializer: Deserializer<V>,
    private val meterRegistry: CampaignMeterRegistry?,
    private val eventsLogger: EventsLogger?,
) : DatasourceObjectConverter<ConsumerRecords<ByteArray?, ByteArray?>, KafkaConsumerResults<K, V>> {

    private var consumedKeyBytesCounter: Counter? = null

    private var consumedValueBytesCounter: Counter? = null

    private var consumedRecordsCounter: Counter? = null

    private val eventPrefix: String = "kafka.consume"

    private val meterPrefix: String = "kafka-consume"

    private lateinit var metersTags: Map<String, String>

    private lateinit var eventsTags: Map<String, String>

    override fun start(context: StepStartStopContext) {
        eventsTags = context.toEventTags()
        metersTags = context.toMetersTags()
        meterRegistry?.apply {
            val scenarioName = context.scenarioName
            val stepName = context.stepName
            consumedKeyBytesCounter = counter(scenarioName, stepName, "$meterPrefix-key-bytes", metersTags).report {
                display(
                    format = "received: %,.0f key bytes",
                    severity = ReportMessageSeverity.INFO,
                    row = 0,
                    column = 2,
                    Counter::count
                )
            }
            consumedValueBytesCounter = counter(scenarioName, stepName, "$meterPrefix-value-bytes", metersTags).report {
                display(
                    format = "received: %,.0f value bytes",
                    severity = ReportMessageSeverity.INFO,
                    row = 0,
                    column = 1,
                    Counter::count
                )
            }
            consumedRecordsCounter = counter(scenarioName, stepName, "$meterPrefix-records", metersTags).report {
                display(
                    format = "received rec: %,.0f",
                    severity = ReportMessageSeverity.INFO,
                    row = 0,
                    column = 0,
                    Counter::count
                )
            }
        }
    }


    override fun stop(context: StepStartStopContext) {
        meterRegistry?.apply {
            consumedKeyBytesCounter = null
            consumedValueBytesCounter = null
            consumedRecordsCounter = null
        }
    }

    override suspend fun supply(
        offset: AtomicLong, value: ConsumerRecords<ByteArray?, ByteArray?>,
        output: StepOutput<KafkaConsumerResults<K, V>>,
    ) {
        val kafkaConsumerMeters = KafkaConsumerMeters(recordsCount = value.count())
        val kafkaConsumerRecordList: List<KafkaConsumerRecord<K, V>> = value.map { record ->
            val keySize = record.serializedKeySize()
            val valueSize = record.serializedValueSize()
            if (keySize > 0) {
                kafkaConsumerMeters.keysBytesReceived += keySize
            }
            if (valueSize > 0) {
                kafkaConsumerMeters.valuesBytesReceived += valueSize
            }

            KafkaConsumerRecord(
                offset.getAndIncrement(),
                record,
                keyDeserializer.deserialize(record.topic(), record.headers(), record.key()),
                valueDeserializer.deserialize(record.topic(), record.headers(), record.value())
            )
        }
        consumedRecordsCounter?.increment(value.count().toDouble())
        consumedKeyBytesCounter?.increment(kafkaConsumerMeters.keysBytesReceived.toDouble())
        consumedValueBytesCounter?.increment(kafkaConsumerMeters.valuesBytesReceived.toDouble())

        eventsLogger?.info("${eventPrefix}.consumed.records", kafkaConsumerMeters.recordsCount, tags = eventsTags)
        eventsLogger?.info(
            "${eventPrefix}.consumed.key-bytes",
            kafkaConsumerMeters.keysBytesReceived,
            tags = eventsTags
        )
        eventsLogger?.info(
            "${eventPrefix}.consumed.value-bytes",
            kafkaConsumerMeters.valuesBytesReceived,
            tags = eventsTags
        )
        tryAndLogOrNull(log) {
            output.send(
                KafkaConsumerResults(
                    kafkaConsumerRecordList,
                    kafkaConsumerMeters
                )
            )
        }

    }

    companion object {

        @JvmStatic
        private val log = logger()
    }
}