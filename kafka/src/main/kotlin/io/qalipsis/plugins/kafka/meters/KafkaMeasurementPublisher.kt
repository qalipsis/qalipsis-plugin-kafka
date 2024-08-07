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
            producer.close(Duration.ofSeconds(10))
        }
    }

    override suspend fun publish(meters: Collection<MeterSnapshot>) {
        try {
            meters.stream().forEach { producer.send(ProducerRecord(topic, it)) }
            log.debug { "Successfully sent ${meters.size} meters to Kafka" }
        } catch (e: Throwable) {
            log.error(e) { "Failed to send metrics to Kafka" }
        }
    }

    private companion object {

        val log = logger()
    }
}
