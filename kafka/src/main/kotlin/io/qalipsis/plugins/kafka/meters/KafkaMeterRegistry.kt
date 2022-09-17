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

import io.micrometer.core.instrument.Clock
import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.step.StepMeterRegistry
import io.qalipsis.api.lang.tryAndLogOrNull
import io.qalipsis.api.logging.LoggerHelper.logger
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serdes
import java.time.Duration
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit

/**
 * [io.micrometer.core.instrument.MeterRegistry] for Apache Kafka.
 *
 * @author Palina Bril
 */
internal class KafkaMeterRegistry(
    private val config: KafkaMeterConfig,
    clock: Clock
) : StepMeterRegistry(config, clock) {

    private lateinit var producer: Producer<ByteArray, Meter>

    private val topic = config.topic()

    override fun start(threadFactory: ThreadFactory) {
        producer = KafkaProducer(
            config.configuration(),
            Serdes.ByteArray().serializer(),
            JsonMeterSerializer(this, baseTimeUnit, config.timestampFieldName())
        )
        super.start(threadFactory)
    }

    override fun stop() {
        tryAndLogOrNull(log) {
            producer.close(Duration.ofSeconds(10))
        }
    }

    override fun getBaseTimeUnit(): TimeUnit {
        return TimeUnit.MILLISECONDS
    }

    public override fun publish() {
        try {
            meters.stream().forEach { producer.send(ProducerRecord(topic, it)) }
            log.debug { "Successfully sent ${meters.size} meters to Kafka" }
        } catch (e: Throwable) {
            log.error(e) { "Failed to send metrics to Kafka" }
        }
    }

    fun getName(meter: Meter): String {
        return getConventionName(meter.id)
    }

    fun getTags(meter: Meter): MutableList<Tag> {
        return getConventionTags(meter.id)
    }

    private companion object {

        val log = logger()
    }
}
