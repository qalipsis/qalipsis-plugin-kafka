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
