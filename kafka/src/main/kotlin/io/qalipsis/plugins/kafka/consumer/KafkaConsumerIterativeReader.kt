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

import io.qalipsis.api.context.StepName
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.steps.datasource.DatasourceIterativeReader
import kotlinx.coroutines.channels.Channel
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.InterruptException
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.Serdes
import java.time.Duration
import java.util.Properties
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern
import javax.validation.constraints.Positive

/**
 * Implementation of [DatasourceIterativeReader] to poll records from Kafka topics.
 *
 * This implementation supports multithreading (concurrent consumers) using the property [concurrency].
 *
 * @author Eric Jessé
 */
internal class KafkaConsumerIterativeReader(
    private val stepName: StepName,
    private val props: Properties,
    @Positive private val pollTimeout: Duration,
    @Positive private val concurrency: Int,
    private val topics: Collection<String>,
    private val topicsPattern: Pattern?
) : DatasourceIterativeReader<ConsumerRecords<ByteArray?, ByteArray?>> {

    private lateinit var executorService: ExecutorService

    private val records = Channel<ConsumerRecords<ByteArray?, ByteArray?>>(Channel.UNLIMITED)

    private var running = false

    private val consumers = mutableListOf<KafkaConsumer<*, *>>()

    private lateinit var stepEventsTags: Map<String, String>

    override fun start(context: StepStartStopContext) {
        stepEventsTags = context.toEventTags()
        running = true
        consumers.clear()
        executorService = Executors.newFixedThreadPool(concurrency)
        repeat(concurrency) { index ->
            try {
                startConsumer(index)
            } catch (e: Exception) {
                log.error(e) { "An error occurred in the step $stepName while starting the consumer: ${e.message}" }
                throw e
            }
        }
    }

    private fun startConsumer(index: Int) {
        val threadsProperties = Properties()
        threadsProperties.putAll(props)

        if (props.containsKey(ConsumerConfig.CLIENT_ID_CONFIG)) {
            threadsProperties[ConsumerConfig.CLIENT_ID_CONFIG] = "${props[ConsumerConfig.CLIENT_ID_CONFIG]}"
        } else {
            threadsProperties[ConsumerConfig.CLIENT_ID_CONFIG] = "qalipsis-step-$stepName"
        }

        if (concurrency > 1) {
            threadsProperties[ConsumerConfig.CLIENT_ID_CONFIG] = "${props[ConsumerConfig.CLIENT_ID_CONFIG]}-$index"
        }

        val kafkaConsumer = KafkaConsumer(
            threadsProperties, Serdes.ByteArray().deserializer(),
            Serdes.ByteArray().deserializer()
        )
        consumers.add(kafkaConsumer)

        // Subscription to topics, partitions and pattern are mutually exclusive
        if (topicsPattern != null) {
            kafkaConsumer.subscribe(topicsPattern)
        } else {
            kafkaConsumer.subscribe(topics)
        }

        log.debug { "Starting polling loop for client ${threadsProperties[ConsumerConfig.CLIENT_ID_CONFIG]}" }
        executorService.submit {
            try {
                while (running) {
                    val consumerRecords = kafkaConsumer.poll(pollTimeout)
                    if (!consumerRecords.isEmpty) {
                        log.trace { "Received ${consumerRecords.count()} records" }
                        records.trySend(consumerRecords).getOrThrow()
                        kafkaConsumer.commitSync()
                    }
                }
            } catch (e: InterruptException) {
                // Ignore for shutdown.
            } catch (e: WakeupException) {
                // Ignore for shutdown.
            } catch (e: Exception) {
                log.error(e) { "An error occurred in the step $stepName: ${e.message}" }
            } finally {
                log.debug { "Closing the polling loop for client ${threadsProperties[ConsumerConfig.CLIENT_ID_CONFIG]}" }
                try {
                    kafkaConsumer.close(CLOSE_TIMEOUT)
                } catch (e: Exception) {
                    // Ignore for shutdown.
                }
                log.debug { "Polling loop for client ${threadsProperties[ConsumerConfig.CLIENT_ID_CONFIG]} is closed" }
            }
        }
    }

    override fun stop(context: StepStartStopContext) {
        log.debug { "Stopping the Kafka consumer for step $stepName" }
        running = false
        // Sends a wake-up event in order to trigger the close of the consumer in its own thread.
        consumers.forEach { it.wakeup() }
        consumers.clear()
        executorService.shutdown()
        executorService.awaitTermination(
            2 * (pollTimeout.toMillis() + CLOSE_TIMEOUT.toMillis()),
            TimeUnit.MILLISECONDS
        )
        log.debug { "Kafka consumer for step $stepName was stopped" }
    }

    override suspend fun hasNext(): Boolean {
        return running
    }

    override suspend fun next(): ConsumerRecords<ByteArray?, ByteArray?> {
        return records.receive()
    }

    companion object {

        private val CLOSE_TIMEOUT = Duration.ofSeconds(10)

        @JvmStatic
        private val log = logger()
    }
}