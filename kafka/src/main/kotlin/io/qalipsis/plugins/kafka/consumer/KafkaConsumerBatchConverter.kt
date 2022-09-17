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

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.qalipsis.api.context.StepOutput
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.lang.tryAndLogOrNull
import io.qalipsis.api.logging.LoggerHelper.logger
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
    private val meterRegistry: MeterRegistry?,
    private val eventsLogger: EventsLogger?,
) : DatasourceObjectConverter<ConsumerRecords<ByteArray?, ByteArray?>, KafkaConsumerResults<K, V>> {

    private var consumedKeyBytesCounter: Counter? = null

    private var consumedValueBytesCounter: Counter? = null

    private var consumedRecordsCounter: Counter? = null

    private val eventPrefix: String = "kafka.consume"

    private val meterPrefix: String = "kafka-consume"

    private lateinit var eventTags: Map<String, String>

    override fun start(context: StepStartStopContext) {
        meterRegistry?.apply {
            val tags = context.toMetersTags()
            consumedKeyBytesCounter = counter("$meterPrefix-key-bytes", tags)
            consumedValueBytesCounter = counter("$meterPrefix-value-bytes", tags)
            consumedRecordsCounter = counter("$meterPrefix-records", tags)
        }
        eventTags = context.toEventTags();
    }


    override fun stop(context: StepStartStopContext) {
        meterRegistry?.apply {
            remove(consumedKeyBytesCounter!!)
            remove(consumedValueBytesCounter!!)
            remove(consumedRecordsCounter!!)
            consumedKeyBytesCounter = null
            consumedValueBytesCounter = null
            consumedRecordsCounter = null
        }
    }

    override suspend fun supply(
        offset: AtomicLong, value: ConsumerRecords<ByteArray?, ByteArray?>,
        output: StepOutput<KafkaConsumerResults<K, V>>
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

        eventsLogger?.info("${eventPrefix}.consumed.records", kafkaConsumerMeters.recordsCount, tags = eventTags)
        eventsLogger?.info("${eventPrefix}.consumed.key-bytes", kafkaConsumerMeters.keysBytesReceived, tags = eventTags)
        eventsLogger?.info(
            "${eventPrefix}.consumed.value-bytes",
            kafkaConsumerMeters.valuesBytesReceived,
            tags = eventTags
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