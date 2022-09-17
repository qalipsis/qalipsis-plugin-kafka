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

import assertk.all
import assertk.assertThat
import assertk.assertions.containsExactly
import assertk.assertions.hasSameSizeAs
import io.micrometer.core.instrument.MeterRegistry
import io.mockk.every
import io.qalipsis.api.events.*
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.plugins.kafka.Constants
import io.qalipsis.test.mockk.relaxedMockk
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.serialization.Serdes
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.*
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.io.PrintWriter
import java.nio.charset.StandardCharsets
import java.time.*
import java.util.*
import kotlin.math.pow

@Testcontainers
internal class KafkaEventsPublisherIntegrationTest {

    private lateinit var bootstrapServers: String

    private lateinit var consumer: KafkaConsumer<ByteArray, String>

    private lateinit var configuration: KafkaEventsConfiguration

    private val meterRegistry: MeterRegistry = relaxedMockk()

    private val eventsConverter = EventJsonConverter()

    @BeforeAll
    internal fun setUp() {
        bootstrapServers = container.bootstrapServers.substring("PLAINTEXT://".length)
        val properties = Properties().also {
            it[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
            it[ConsumerConfig.GROUP_ID_CONFIG] = "test-group"
            it[ConsumerConfig.CLIENT_ID_CONFIG] = "test-client"
            it[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "${OffsetResetStrategy.EARLIEST}".lowercase()
            it[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = false
        }
        configuration = KafkaEventsConfiguration().apply {
            this.bootstrap = bootstrapServers
        }
        consumer = KafkaConsumer(properties, Serdes.ByteArray().deserializer(), Serdes.String().deserializer())
    }

    @AfterAll
    internal fun tearDown() {
        consumer.close()
    }

    @Test
    @Timeout(30)
    internal fun `should export data`() {
        // given
        val publisher = KafkaEventsPublisher(configuration, meterRegistry, eventsConverter)
        publisher.start()

        val events = mutableListOf<Event>()
        events.add(Event(name = "my-event", EventLevel.WARN))
        events.add(
            Event(
                name = "my-event",
                EventLevel.DEBUG,
                tags = listOf(EventTag("key-1", "value-1"), EventTag("key-2", "value-2"))
            )
        )

        val values = createTestData()
        values.forEachIndexed { index, value ->
            events.add(Event(name = "my-event-$index", EventLevel.INFO, value = value))
        }

        // when
        events.forEach(publisher::publish)

        // then
        val published = mutableListOf<String>()
        consumer.subscribe(listOf(configuration.topic))
        while (published.size < values.size) {
            published.addAll(
                consumer.poll(Duration.ofSeconds(10))
                    .map(ConsumerRecord<ByteArray, String>::value)
            )
        }
        consumer.unsubscribe()

        // Verification of the overall values.
        val serializer = JsonEventSerializer(eventsConverter)
        assertThat(published).all {
            hasSameSizeAs(events)
            containsExactly(*events.map { serializer.serialize("", it).toString(StandardCharsets.UTF_8) }
                .toTypedArray())
        }
    }

    /**
     * Create the test data set with the value to log as key and the condition to match when asserting the data as value.
     */
    private fun createTestData() = listOf(
        "my-message",
        true,
        123.65,
        Double.POSITIVE_INFINITY,
        Double.NEGATIVE_INFINITY,
        Double.NaN,
        123.65.toFloat(),
        123.65.toBigDecimal(),
        123,
        123.toBigInteger(),
        123.toLong(),
        Instant.now().minusSeconds(12),
        ZonedDateTime.now(),
        LocalDateTime.now().plusDays(1),
        relaxedMockk<Throwable> {
            every { message } returns "my-error"
            every { printStackTrace(any<PrintWriter>()) } answers {
                (firstArg() as PrintWriter).write("this is the stack")
            }
        },
        Duration.ofSeconds(12),
        EventGeoPoint(12.34, 34.76),
        EventRange(12.34, 34.76, includeUpper = false),
        arrayOf(12.34, "here is the test"),
        listOf(12.34, "here is the test"),
        MyTestObject()
    )

    data class MyTestObject(val property1: Double = 1243.65, val property2: String = "here is the test")

    companion object {

        @Container
        @JvmStatic
        private val container = KafkaContainer(DockerImageName.parse(Constants.DOCKER_IMAGE)).apply {
            withCreateContainerCmdModifier { cmd ->
                cmd.hostConfig!!.withMemory(512 * 1024.0.pow(2).toLong()).withCpuCount(2)
            }
            withEnv("KAFKA_HEAP_OPTS", "-Xms256m -Xmx256m")
        }

        @JvmStatic
        private val log = logger()
    }

}
