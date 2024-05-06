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

package io.qalipsis.plugins.kafka.events

import io.micronaut.context.annotation.Requires
import io.qalipsis.api.events.Event
import io.qalipsis.api.events.EventJsonConverter
import io.qalipsis.api.events.EventsPublisher
import io.qalipsis.api.lang.tryAndLogOrNull
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.meters.CampaignMeterRegistry
import io.qalipsis.api.meters.Counter
import jakarta.inject.Singleton
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serdes
import java.time.Duration
import java.util.Properties

/**
 * Implementation of [io.qalipsis.api.events.EventsLogger] for Kafka.
 * The structure of the data is similar to the one in Elasticsearch to allow asynchronous via Logstash.
 * However, this does not prevent from
 *
 * @author Eric Jessé
 */
@Singleton
@Requires(beans = [KafkaEventsConfiguration::class])
internal class KafkaEventsPublisher(
    private val configuration: KafkaEventsConfiguration,
    private val meterRegistry: CampaignMeterRegistry,
    private val eventsConverter: EventJsonConverter
) : EventsPublisher {

    private lateinit var producer: Producer<ByteArray, Event>

    private lateinit var counter: Counter

    override fun start() {
        val actualProperties = Properties()
        actualProperties.putAll(configuration.configuration)
        actualProperties[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = configuration.bootstrap
        actualProperties[ProducerConfig.LINGER_MS_CONFIG] = configuration.lingerPeriod.toMillis().toString()
        actualProperties[ProducerConfig.BATCH_SIZE_CONFIG] = configuration.batchSize.toString()
        producer =
            KafkaProducer(actualProperties, Serdes.ByteArray().serializer(), JsonEventSerializer(eventsConverter))
        counter = meterRegistry.counter(
            scenarioName = "",
            stepName = "",
            name = EVENTS_EXPORT_TIMER_NAME,
        )
        super.start()
    }

    override fun stop() {
        tryAndLogOrNull(log) {
            producer.close(Duration.ofSeconds(10))
        }
        meterRegistry.clear()
    }

    override fun publish(event: Event) {
        producer.send(ProducerRecord(configuration.topic, event))
        counter.increment()
    }

    companion object {

        private const val EVENTS_EXPORT_TIMER_NAME = "kafka.events.export"

        @JvmStatic
        private val log = logger()
    }

}
