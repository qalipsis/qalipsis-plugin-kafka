package io.qalipsis.plugins.kafka.micrometer

import assertk.all
import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.isEqualTo
import assertk.assertions.isGreaterThan
import assertk.assertions.isNotNull
import assertk.assertions.key
import assertk.assertions.prop
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import io.micrometer.core.instrument.Clock
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.util.NamedThreadFactory
import io.qalipsis.api.events.*
import io.qalipsis.plugins.kafka.Constants
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serdes
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.*
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.time.*
import java.util.*
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.pow

@Testcontainers
internal class KafkaMeterRegistryIntegrationTest {

    private lateinit var bootstrapServers: String

    private lateinit var consumer: KafkaConsumer<ByteArray, Meter>

    private lateinit var configuration: KafkaMeterConfig

    @BeforeAll
    internal fun setUpAll() {
        bootstrapServers = CONTAINER.bootstrapServers.substringAfter("PLAINTEXT://")
        val consumerProperties = Properties()
        consumerProperties[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        consumerProperties[ConsumerConfig.GROUP_ID_CONFIG] = "test"
        consumerProperties[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "${OffsetResetStrategy.EARLIEST}".lowercase()
        consumerProperties[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = false
        consumer = KafkaConsumer(consumerProperties, Serdes.ByteArray().deserializer(), JacksonMeterDeserializer())

        val meterRegistryProperties = Properties()
        meterRegistryProperties["kafka.${ProducerConfig.BOOTSTRAP_SERVERS_CONFIG}"] = bootstrapServers
        meterRegistryProperties["kafka.step"] = "1s"

        configuration = object : KafkaMeterConfig() {
            override fun get(key: String?): String? {
                return meterRegistryProperties.getProperty(key)
            }
        }
    }

    @AfterEach
    internal fun tearDown() {
        consumer.unsubscribe()
    }

    @AfterAll
    internal fun tearDownAll() {
        consumer.close()
    }

    @Test
    @Timeout(30)
    internal fun `should export data`() {
        // given
        val meterRegistry = KafkaMeterRegistry(configuration, Clock.SYSTEM)
        val beforePublication = Instant.now()
        meterRegistry.start(DEFAULT_THREAD_FACTORY)
        meterRegistry.counter("the-counter", Tags.of("tag-1", "value-2")).increment(8.0)
        meterRegistry.timer("the-timer", Tags.of("tag-2", "value-3")).run {
            record(Duration.ofMillis(2))
            record(Duration.ofMillis(12))
        }
        meterRegistry.gauge("the-gauge", Tags.of("tag-3", "value-3"), AtomicInteger())!!.addAndGet(654)

        // when
        val published = mutableListOf<Meter>()
        consumer.subscribe(listOf(configuration.topic()))
        do {
            val consumed = consumer.poll(Duration.ofSeconds(10)).map(ConsumerRecord<ByteArray, Meter>::value)
            published += consumed
        } while (published.size < 3)

        // then
        // We keep only the values of the first step, because the timer is only affected by our operation
        // in that period of time.
        val firstPublishedValues = published.sortedBy { it.timestamp }.distinctBy { it::class }
        val timer = firstPublishedValues.filterIsInstance<Timer>().first()
        val gauge = firstPublishedValues.filterIsInstance<Gauge>().first()
        val counter = firstPublishedValues.filterIsInstance<Counter>().first()
        assertThat(timer).all {
            prop(Timer::timestamp).isNotNull().isGreaterThan(beforePublication)
            prop(Timer::count).isEqualTo(2)
            prop(Timer::sum).isEqualTo(14.0)
            prop(Timer::max).isEqualTo(12.0)
            prop(Timer::name).isEqualTo("the-timer")
            prop(Timer::mean).isEqualTo(7.0)
            prop(Timer::tags).all {
                hasSize(1)
                key("tag-2").isEqualTo("value-3")
            }
        }
        assertThat(gauge).all {
            prop(Gauge::timestamp).isNotNull().isGreaterThan(beforePublication)
            prop(Gauge::value).isEqualTo(654.0)
            prop(Gauge::name).isEqualTo("the-gauge")
            prop(Gauge::tags).all {
                hasSize(1)
                key("tag-3").isEqualTo("value-3")
            }
        }
        assertThat(counter).all {
            prop(Counter::timestamp).isNotNull().isGreaterThan(beforePublication)
            prop(Counter::count).isEqualTo(8)
            prop(Counter::name).isEqualTo("the-counter")
            prop(Counter::tags).all {
                hasSize(1)
                key("tag-1").isEqualTo("value-2")
            }
        }
    }

    class JacksonMeterDeserializer() : Deserializer<Meter> {

        private val objectMapper = jacksonObjectMapper().apply {
            registerModule(JavaTimeModule())
        }

        override fun deserialize(topic: String?, data: ByteArray): Meter {
            return objectMapper.readValue(data)
        }
    }

    companion object {

        val DEFAULT_THREAD_FACTORY = NamedThreadFactory("kafka-metrics-publisher")

        @Container
        @JvmStatic
        private val CONTAINER = KafkaContainer(DockerImageName.parse(Constants.DOCKER_IMAGE)).apply {
            withCreateContainerCmdModifier { cmd ->
                cmd.hostConfig!!.withMemory(512 * 1024.0.pow(2).toLong()).withCpuCount(2)
            }
            withEnv("KAFKA_HEAP_OPTS", "-Xms256m -Xmx256m")
        }
    }
}
