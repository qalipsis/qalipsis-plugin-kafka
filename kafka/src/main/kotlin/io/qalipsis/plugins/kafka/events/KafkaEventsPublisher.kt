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
        counter = meterRegistry.counter(EVENTS_EXPORT_TIMER_NAME)
        super.start()
    }

    override fun stop() {
        tryAndLogOrNull(log) {
            producer.flush()
            producer.close(Duration.ofSeconds(10))
        }
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
