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

package io.qalipsis.plugins.kafka.producer

import io.qalipsis.api.context.StepContext
import io.qalipsis.api.context.StepName
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.lang.tryAndLog
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.meters.CampaignMeterRegistry
import io.qalipsis.api.meters.Counter
import io.qalipsis.api.report.ReportMessageSeverity
import io.qalipsis.api.retry.RetryPolicy
import io.qalipsis.api.steps.AbstractStep
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Serializer
import java.time.Duration
import java.util.*

/**
 * Implementation of a [io.qalipsis.api.steps.Step] able to produce a message into Kafka broker.
 *
 * @property stepId id of the step.
 * @property retryPolicy of the step.
 * @property clientName used to identify the client communicating with the broker.
 * @property props kafka configuration properties.
 * @property recordsFactory closure to generate the records to be published.
 * @property metrics for the producer step.
 * @property keySerializer serializer for the kafka records keys.
 * @property valuesSerializer serializer for the kafka records values.
 *
 * @author Gabriel Moraes
 */
internal class KafkaProducerStep<I, K, V>(
    private val stepId: StepName,
    retryPolicy: RetryPolicy?,
    private val clientName: String?,
    private val props: Properties,
    private val recordsFactory: suspend (ctx: StepContext<*, *>, input: I) -> List<KafkaProducerRecord<K, V>>,
    private val keySerializer: Serializer<K>,
    private val valuesSerializer: Serializer<V>,
    private val eventsLogger: EventsLogger?,
    private val meterRegistry: CampaignMeterRegistry?,
) : AbstractStep<I, KafkaProducerResult<I>>(stepId, retryPolicy) {

    private lateinit var kafkaProducer: KafkaProducer<K, V>

    private val eventPrefix = "kafka.produce"

    private val meterPrefix = "kafka-produce"

    private var keysBytesSent: Counter? = null

    private var valuesBytesSent: Counter? = null

    private var recordsCount: Counter? = null

    override suspend fun start(context: StepStartStopContext) {
        val tags = context.toMetersTags()
        val scenarioName = context.scenarioName
        val stepName = context.stepName
        meterRegistry?.apply {
            recordsCount = counter(scenarioName, stepName, "$meterPrefix-records", tags).report {
                display(
                    format = "produced rec: %,.0f",
                    severity = ReportMessageSeverity.INFO,
                    row = 0,
                    column = 0,
                    Counter::count
                )
            }
            keysBytesSent = counter(scenarioName, stepName, "$meterPrefix-key-bytes", tags).report {
                display(
                    format = "produced: %,.0f keys bytes",
                    severity = ReportMessageSeverity.INFO,
                    row = 0,
                    column = 2,
                    Counter::count
                )
            }
            valuesBytesSent = counter(scenarioName, stepName, "$meterPrefix-value-bytes", tags).report {
                display(
                    format = "produced: %,.0f values bytes",
                    severity = ReportMessageSeverity.INFO,
                    row = 0,
                    column = 1,
                    Counter::count
                )
            }
        }
        kafkaProducer = buildProducer()
    }

    private fun buildProducer(): KafkaProducer<K, V> {
        val kafkaProperties = Properties().also { kafkaProp ->
            kafkaProp[ProducerConfig.CLIENT_ID_CONFIG] = clientName ?: "qalipsis-producer-$stepId"
        }
        kafkaProperties.putAll(props)
        return KafkaProducer(kafkaProperties, keySerializer, valuesSerializer)
    }

    override suspend fun execute(context: StepContext<I, KafkaProducerResult<I>>) {
        val input = context.receive()

        val records = recordsFactory(context, input)

        val metersForCall = KafkaProducerMeters()

        records.forEach {
            val record = it.toProducerRecord()

            record.key()?.let { key ->
                metersForCall.keysBytesSent += keySerializer.serialize(record.topic(), key).size
            }
            metersForCall.valuesBytesSent += valuesSerializer.serialize(record.topic(), record.value()).size
            kafkaProducer.send(record)
        }

        recordsCount?.increment(records.count().toDouble())
        keysBytesSent?.increment(metersForCall.keysBytesSent.toDouble())
        valuesBytesSent?.increment(metersForCall.valuesBytesSent.toDouble())

        eventsLogger?.info("${eventPrefix}.sent.records", records.count(), tags = context.toEventTags())
        eventsLogger?.info("${eventPrefix}.sent.keys-bytes", metersForCall.keysBytesSent, tags = context.toEventTags())
        eventsLogger?.info(
            "${eventPrefix}.sent.values-bytes",
            metersForCall.valuesBytesSent,
            tags = context.toEventTags()
        )

        context.send(KafkaProducerResult(input, metersForCall))
    }

    override suspend fun stop(context: StepStartStopContext) {
        tryAndLog(log) { kafkaProducer.close(CLOSE_TIMEOUT) }
        meterRegistry?.apply {
            recordsCount = null
            keysBytesSent = null
            valuesBytesSent = null
        }
    }

    companion object {

        private val CLOSE_TIMEOUT = Duration.ofSeconds(10)

        @JvmStatic
        private val log = logger()
    }

}