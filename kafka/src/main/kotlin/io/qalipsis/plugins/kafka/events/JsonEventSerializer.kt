package io.qalipsis.plugins.kafka.events

import io.qalipsis.api.events.Event
import io.qalipsis.api.events.EventJsonConverter
import org.apache.kafka.common.serialization.Serializer
import java.nio.charset.StandardCharsets

/**
 * Kafka serializer for the events as JSON.
 */
internal class JsonEventSerializer(
    private val eventsConverter: EventJsonConverter
) : Serializer<Event> {

    override fun serialize(topic: String, data: Event): ByteArray {
        return eventsConverter.convert(data).toByteArray(StandardCharsets.UTF_8)
    }

}
