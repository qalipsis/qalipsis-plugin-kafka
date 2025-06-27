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

import io.micronaut.context.annotation.Requires
import io.qalipsis.api.lang.tryAndLogOrNull
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.meters.MeasurementPublisher
import io.qalipsis.api.meters.MeterSnapshot
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serdes
import java.time.Duration

/**
 * Measurement publisher for Apache Kafka.
 *
 * @author Palina Bril
 */
@Requires(beans = [KafkaMeterConfig::class])
internal class KafkaMeasurementPublisher(
    private val config: KafkaMeterConfig,
) : MeasurementPublisher {

    private lateinit var producer: Producer<ByteArray, MeterSnapshot>

    private val topic = config.topic

    override suspend fun init() {
        producer = KafkaProducer(
            config.configuration(),
            Serdes.ByteArray().serializer(),
            JsonMeterSerializer(config.timestampFieldName)
        )
    }
    override suspend fun stop() {
        tryAndLogOrNull(log) {
            producer.flush()
            producer.close(Duration.ofSeconds(10))
        }
    }

    override suspend fun publish(meters: Collection<MeterSnapshot>) {
        try {
            meters.forEach { producer.send(ProducerRecord(topic, it)) }
            log.debug { "Successfully sent ${meters.size} meters to Kafka" }
        } catch (e: Throwable) {
            log.error(e) { "Failed to send metrics to Kafka" }
        }
    }

    private companion object {

        val log = logger()
    }
}
