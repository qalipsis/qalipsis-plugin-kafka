package io.qalipsis.plugins.kafka.events

import assertk.all
import assertk.assertThat
import assertk.assertions.any
import assertk.assertions.hasSize
import assertk.assertions.isEmpty
import assertk.assertions.isEqualTo
import assertk.assertions.isInstanceOf
import assertk.assertions.isTrue
import assertk.assertions.key
import assertk.assertions.prop
import io.micronaut.context.ApplicationContext
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.qalipsis.api.events.EventLevel
import io.qalipsis.api.events.EventsPublisher
import jakarta.inject.Inject
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.time.Duration

/**
 * Tests to verify the automatic configuration of the Elasticsearch publisher.
 */
class KafkaEventsPublisherConfigurationIntegrationTest {

    @Nested
    @MicronautTest(propertySources = ["classpath:application-kafka.yml"])
    inner class NoPublisher {

        @Inject
        private lateinit var applicationContext: ApplicationContext

        @Test
        @Timeout(10)
        internal fun `should start without publisher`() {
            assertThat(applicationContext.getBeansOfType(EventsPublisher::class.java)).isEmpty()
            assertThat(applicationContext.getBeansOfType(KafkaEventsPublisher::class.java)).isEmpty()
        }
    }

    @Nested
    @MicronautTest(environments = ["withpublisher"])
    inner class WithPublisher {

        @Inject
        private lateinit var applicationContext: ApplicationContext

        @Test
        @Timeout(10)
        internal fun `should start with the publisher`() {
            assertThat(applicationContext.getBeansOfType(EventsPublisher::class.java)).all {
                hasSize(1)
                any { it.isInstanceOf(KafkaEventsPublisher::class) }
            }
            val configuration = applicationContext.getBean(KafkaEventsConfiguration::class.java)
            assertThat(configuration).all {
                prop(KafkaEventsConfiguration::minLevel).isEqualTo(EventLevel.TRACE)
                prop(KafkaEventsConfiguration::bootstrap).isEqualTo("my-host:6542,other-host:2565")
                prop(KafkaEventsConfiguration::topic).isEqualTo("the-events")
                prop(KafkaEventsConfiguration::durationAsNano).isTrue()
                prop(KafkaEventsConfiguration::lingerPeriod).isEqualTo(Duration.ofSeconds(30))
                prop(KafkaEventsConfiguration::batchSize).isEqualTo(100)
                prop(KafkaEventsConfiguration::configuration).all {
                    hasSize(2)
                    key("config1").isEqualTo("value1")
                    key("config2").isEqualTo("value2")
                }
            }
        }
    }

}
