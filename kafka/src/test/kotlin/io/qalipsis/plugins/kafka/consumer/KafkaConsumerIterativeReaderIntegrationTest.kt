package io.qalipsis.plugins.kafka.consumer

import assertk.all
import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.index
import assertk.assertions.isEqualTo
import assertk.assertions.prop
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.plugins.kafka.Constants
import io.qalipsis.test.mockk.relaxedMockk
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serdes
import org.junit.jupiter.api.*
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.*
import java.util.regex.Pattern
import kotlin.math.pow

/**
 *
 * @author Eric Jessé
 */
@Testcontainers
internal class KafkaConsumerIterativeReaderIntegrationTest {

    private val bootstrap by lazy(LazyThreadSafetyMode.NONE) {
        container.bootstrapServers.substring("PLAINTEXT://".length)
    }

    private val kafkaProducer by lazy(LazyThreadSafetyMode.NONE) {
        KafkaProducer(Properties().also {
            it[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrap
            it[ProducerConfig.COMPRESSION_TYPE_CONFIG] = "lz4"
            it[ProducerConfig.LINGER_MS_CONFIG] = 50
            it[ProducerConfig.CLIENT_ID_CONFIG] = "test-client"
        }, Serdes.String().serializer(), Serdes.String().serializer())
    }

    private val adminConfig by lazy(LazyThreadSafetyMode.NONE) {
        Properties().also {
            it[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrap
            it[AdminClientConfig.CLIENT_ID_CONFIG] = "admin-client"
        }
    }

    private val adminClient by lazy(LazyThreadSafetyMode.NONE) {
        AdminClient.create(adminConfig)
    }

    private val consumerConfig by lazy(LazyThreadSafetyMode.NONE) {
        Properties().also {
            it[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrap
            it[ConsumerConfig.GROUP_ID_CONFIG] = "test-group"
            it[ConsumerConfig.CLIENT_ID_CONFIG] = "test-client"
            it[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "${OffsetResetStrategy.EARLIEST}".lowercase()
            it[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = false
        }
    }

    private lateinit var reader: KafkaConsumerIterativeReader

    @AfterEach
    @Timeout(10)
    internal fun tearDown() {
        reader.stop(relaxedMockk())

        adminClient.deleteTopics(adminClient.listTopics().names().get())

        // Wait until the topics are actually deleted.
        while (adminClient.listTopics().names().get().also {
                log.debug { "Current list of existing topics: ${it.joinToString(", ")}" }
            }.isNotEmpty()) {
            Thread.sleep(500)
        }
        log.info { "All topics deleted" }
    }

    private fun createTopics(topics: Collection<String>) {
        log.info { "Creating topics ${topics.joinToString(", ")}" }
        adminClient.createTopics(topics.map { NewTopic(it, 1, 1) }).all().get()

        // Wait until the topics are actually created.
        while (!adminClient.listTopics().names().get().also {
                log.debug { "Current list of existing topics: ${it.joinToString(", ")}" }
            }.containsAll(topics)) {
            Thread.sleep(500)
        }

        log.info { "Topics ${topics.joinToString(", ")} created" }
    }

    @Test
    @Timeout(10)
    internal fun `should consume all the data from subscribed topics only`() = runBlocking {
        // given
        createTopics((1..10).map { "topic-$it" })
        (1..10).forEach {
            kafkaProducer.send(ProducerRecord("topic-$it", "key-$it", "value-$it"))
        }
        reader = KafkaConsumerIterativeReader(
            "any",
            consumerConfig,
            Duration.ofMillis(100),
            1,
            listOf("topic-5", "topic-8"),
            null
        )
        reader.start(relaxedMockk())

        // when
        val received = mutableListOf<ConsumerRecord<ByteArray?, ByteArray?>>()
        while (received.size < 2) {
            val records = reader.next()
            received.addAll(records)
        }

        // then
        assertThat(received).transform { it.sortedBy { it.topic() } }.all {
            hasSize(2)
            index(0).all {
                prop(ConsumerRecord<ByteArray?, ByteArray?>::topic).isEqualTo("topic-5")
                prop(ConsumerRecord<ByteArray?, ByteArray?>::partition).isEqualTo(0)
                prop(ConsumerRecord<ByteArray?, ByteArray?>::key).transform { it!!.toString(StandardCharsets.UTF_8) }
                    .isEqualTo("key-5")
                prop(ConsumerRecord<ByteArray?, ByteArray?>::value).transform { it!!.toString(StandardCharsets.UTF_8) }
                    .isEqualTo("value-5")
            }
            index(1).all {
                prop(ConsumerRecord<ByteArray?, ByteArray?>::topic).isEqualTo("topic-8")
                prop(ConsumerRecord<ByteArray?, ByteArray?>::partition).isEqualTo(0)
                prop(ConsumerRecord<ByteArray?, ByteArray?>::key).transform { it!!.toString(StandardCharsets.UTF_8) }
                    .isEqualTo("key-8")
                prop(ConsumerRecord<ByteArray?, ByteArray?>::value).transform { it!!.toString(StandardCharsets.UTF_8) }
                    .isEqualTo("value-8")
            }
        }

        // No other message will be read.
        assertThrows<TimeoutCancellationException> {
            withTimeout(1000) {
                reader.next()
            }
        }
    }

    @Test
    @Timeout(10)
    internal fun `should consume all the data from subscribed topics pattern only`() = runBlocking {
        // given
        createTopics((1..20).map { "topic-$it" })
        (1..20).forEach {
            kafkaProducer.send(ProducerRecord("topic-$it", "key-$it", "value-$it"))
        }
        reader = KafkaConsumerIterativeReader(
            "any",
            consumerConfig,
            Duration.ofMillis(100),
            1,
            emptyList(),
            Pattern.compile("[tT]o.ic-1[2-4]?")
        )
        reader.start(relaxedMockk())

        // when
        val received = mutableListOf<ConsumerRecord<ByteArray?, ByteArray?>>()
        while (received.size < 4) {
            val records = reader.next()
            received.addAll(records)
        }

        // then
        assertThat(received).transform { it.sortedBy { it.topic() } }.all {
            hasSize(4)
            index(0).all {
                prop(ConsumerRecord<ByteArray?, ByteArray?>::topic).isEqualTo("topic-1")
                prop(ConsumerRecord<ByteArray?, ByteArray?>::partition).isEqualTo(0)
                prop(ConsumerRecord<ByteArray?, ByteArray?>::key).transform { it!!.toString(StandardCharsets.UTF_8) }
                    .isEqualTo("key-1")
                prop(ConsumerRecord<ByteArray?, ByteArray?>::value).transform { it!!.toString(StandardCharsets.UTF_8) }
                    .isEqualTo("value-1")
            }
            index(1).all {
                prop(ConsumerRecord<ByteArray?, ByteArray?>::topic).isEqualTo("topic-12")
                prop(ConsumerRecord<ByteArray?, ByteArray?>::partition).isEqualTo(0)
                prop(ConsumerRecord<ByteArray?, ByteArray?>::key).transform { it!!.toString(StandardCharsets.UTF_8) }
                    .isEqualTo("key-12")
                prop(ConsumerRecord<ByteArray?, ByteArray?>::value).transform { it!!.toString(StandardCharsets.UTF_8) }
                    .isEqualTo("value-12")
            }
            index(2).all {
                prop(ConsumerRecord<ByteArray?, ByteArray?>::topic).isEqualTo("topic-13")
                prop(ConsumerRecord<ByteArray?, ByteArray?>::partition).isEqualTo(0)
                prop(ConsumerRecord<ByteArray?, ByteArray?>::key).transform { it!!.toString(StandardCharsets.UTF_8) }
                    .isEqualTo("key-13")
                prop(ConsumerRecord<ByteArray?, ByteArray?>::value).transform { it!!.toString(StandardCharsets.UTF_8) }
                    .isEqualTo("value-13")
            }
            index(3).all {
                prop(ConsumerRecord<ByteArray?, ByteArray?>::topic).isEqualTo("topic-14")
                prop(ConsumerRecord<ByteArray?, ByteArray?>::partition).isEqualTo(0)
                prop(ConsumerRecord<ByteArray?, ByteArray?>::key).transform { it!!.toString(StandardCharsets.UTF_8) }
                    .isEqualTo("key-14")
                prop(ConsumerRecord<ByteArray?, ByteArray?>::value).transform { it!!.toString(StandardCharsets.UTF_8) }
                    .isEqualTo("value-14")
            }
        }

        // No other message will be read.
        assertThrows<TimeoutCancellationException> {
            withTimeout(1000) {
                reader.next()
            }
        }
    }

    @Test
    @Timeout(10)
    internal fun `should always have next at start but not at stop`() = runBlocking {
        reader = KafkaConsumerIterativeReader(
            "any",
            consumerConfig,
            Duration.ofMillis(100),
            1,
            listOf(UUID.randomUUID().toString()),
            null
        )
        reader.start(relaxedMockk())
        Assertions.assertTrue(reader.hasNext())
        reader.stop(relaxedMockk())
        Assertions.assertFalse(reader.hasNext())
    }

    @Test
    @Timeout(10)
    internal fun `should accept start after stop and consume`() = runBlocking {
        createTopics((1..20).map { "topic-$it" })

        reader = KafkaConsumerIterativeReader(
            "any",
            consumerConfig,
            Duration.ofMillis(100),
            1,
            listOf(UUID.randomUUID().toString()),
            Pattern.compile("topic-1.")
        )
        reader.start(relaxedMockk())
        reader.stop(relaxedMockk())

        (1..20).forEach {
            kafkaProducer.send(ProducerRecord("topic-$it", "key-$it", "value-$it"))
        }

        reader.start(relaxedMockk())
        val received = mutableListOf<ConsumerRecord<*, *>>()
        while (received.size < 10) {
            val records = reader.next()
            received.addAll(records)
            log.debug { "Received ${received.size} records" }
        }
    }

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