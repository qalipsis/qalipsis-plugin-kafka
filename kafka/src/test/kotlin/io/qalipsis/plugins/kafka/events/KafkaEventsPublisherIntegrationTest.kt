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

import assertk.all
import assertk.assertThat
import assertk.assertions.containsExactly
import assertk.assertions.hasSameSizeAs
import io.mockk.every
import io.qalipsis.api.events.Event
import io.qalipsis.api.events.EventGeoPoint
import io.qalipsis.api.events.EventJsonConverter
import io.qalipsis.api.events.EventLevel
import io.qalipsis.api.events.EventRange
import io.qalipsis.api.events.EventTag
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.meters.CampaignMeterRegistry
import io.qalipsis.plugins.kafka.Constants
import io.qalipsis.test.mockk.relaxedMockk
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.serialization.Serdes
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.io.PrintWriter
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZonedDateTime
import java.util.Properties
import kotlin.math.pow

@Testcontainers
internal class KafkaEventsPublisherIntegrationTest {

    private lateinit var bootstrapServers: String

    private lateinit var consumer: KafkaConsumer<ByteArray, String>

    private lateinit var configuration: KafkaEventsConfiguration

    private val meterRegistry: CampaignMeterRegistry = relaxedMockk()

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
