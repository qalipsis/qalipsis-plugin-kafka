package io.qalipsis.plugins.kafka.meters

import assertk.all
import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.isEqualTo
import assertk.assertions.key
import assertk.assertions.prop
import io.micronaut.context.ApplicationContext
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.qalipsis.plugins.kafka.meters.KafkaMeterRegistry
import io.qalipsis.test.assertk.typedProp
import jakarta.inject.Inject
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout

@MicronautTest(environments = ["kafka", "kafka-config-test"])
internal class KafkaMeterRegistryConfigIntegrationTest {

    @Inject
    private lateinit var applicationContext: ApplicationContext

    @Test
    @Timeout(10)
    internal fun `should start with the registry`() {
        assertThat(applicationContext.getBean(KafkaMeterRegistry::class.java))
            .typedProp<KafkaMeterConfig>("config").all {
                prop(KafkaMeterConfig::prefix).isEqualTo("kafka")
                prop(KafkaMeterConfig::topic).isEqualTo("qalipsis-meters")
                prop(KafkaMeterConfig::timestampFieldName).isEqualTo("timestamp")
                prop(KafkaMeterConfig::configuration).all {
                    hasSize(4)
                    key("batch.size").isEqualTo(500)
                    key("bootstrap.servers").isEqualTo("kafka-1:9092")
                    key("linger.ms").isEqualTo("3000")
                    key("delivery.timeout.ms").isEqualTo("14000")
                }
            }

    }
}