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
 * Implementation of [DatasourceObjectConverter], that reads a batch of native Kafka records and forwards each of
 * them converted to as [KafkaConsumerRecord].
 *
 * @author Eric Jessé
 */
internal class KafkaConsumerSingleConverter<K, V>(
    private val keyDeserializer: Deserializer<K>,
    private val valueDeserializer: Deserializer<V>,
    private val meterRegistry: CampaignMeterRegistry?,
    private val eventsLogger: EventsLogger?

) : DatasourceObjectConverter<ConsumerRecords<ByteArray?, ByteArray?>, KafkaConsumerResult<K, V>> {

    private var consumedKeyBytesCounter: Counter? = null

    private var consumedValueBytesCounter: Counter? = null

    private var consumedRecordsCounter: Counter? = null

    private val eventPrefix: String = "kafka.consume"

    private val meterPrefix: String = "kafka-consume"

    private lateinit var tags: Map<String, String>


    override fun start(context: StepStartStopContext) {
        meterRegistry?.apply {
            tags = context.toEventTags()
            val scenarioName = context.scenarioName
            val stepName = context.stepName
            consumedKeyBytesCounter = counter(scenarioName, stepName, "$meterPrefix-key-bytes", tags).report {
                display(
                    format = "received: %,.0f key bytes",
                    severity = ReportMessageSeverity.INFO,
                    row = 0,
                    column = 2,
                    Counter::count
                )
            }
            consumedValueBytesCounter = counter(scenarioName, stepName, "$meterPrefix-value-bytes", tags).report {
                display(
                    format = "received: %,.0f values bytes",
                    severity = ReportMessageSeverity.INFO,
                    row = 0,
                    column = 1,
                    Counter::count
                )
            }
            consumedRecordsCounter = counter(scenarioName, stepName, "$meterPrefix-records", tags).report {
                display(
                    format = "received rec: %,.0f",
                    severity = ReportMessageSeverity.INFO,
                    row = 0,
                    column = 0,
                    Counter::count
                )
            }
        }
        tags = context.toEventTags()
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
        output: StepOutput<KafkaConsumerResult<K, V>>
    ) {
        val kafkaConsumerMeters = KafkaConsumerMeters(recordsCount = value.count())

        value.forEach { record ->
            val keySize = record.serializedKeySize()
            if (keySize > 0) {
                kafkaConsumerMeters.keysBytesReceived += keySize
            }
            val valueSize = record.serializedValueSize()
            if (valueSize > 0) {
                kafkaConsumerMeters.valuesBytesReceived += valueSize
            }
            tryAndLogOrNull(log) {
                output.send(
                    KafkaConsumerResult(
                        KafkaConsumerRecord(
                            offset.getAndIncrement(),
                            record,
                            keyDeserializer.deserialize(record.topic(), record.headers(), record.key()),
                            valueDeserializer.deserialize(record.topic(), record.headers(), record.value())
                        ),
                        KafkaConsumerMeters(
                            keySize,
                            valueSize,
                            value.count()
                        )
                    )
                )
            }
        }
        consumedRecordsCounter?.increment(value.count().toDouble())
        consumedKeyBytesCounter?.increment(kafkaConsumerMeters.keysBytesReceived.toDouble())
        consumedValueBytesCounter?.increment(kafkaConsumerMeters.valuesBytesReceived.toDouble())

        eventsLogger?.info("${eventPrefix}.consumed.records", kafkaConsumerMeters.recordsCount, tags = tags)
        eventsLogger?.info("${eventPrefix}.consumed.key-bytes", kafkaConsumerMeters.keysBytesReceived, tags = tags)
        eventsLogger?.info(
            "${eventPrefix}.consumed.value-bytes",
            kafkaConsumerMeters.valuesBytesReceived,
            tags = tags
        )
    }

    companion object {

        @JvmStatic
        private val log = logger()
    }
}