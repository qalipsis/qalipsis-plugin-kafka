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

package io.qalipsis.plugins.kafka.serdes

import com.fasterxml.jackson.annotation.JsonAutoDetect
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.kotlinModule
import io.micronaut.jackson.modules.BeanIntrospectionModule

/**
 * Default JSON Mapper for Kafka.
 *
 * @author Eric Jessé
 */
internal object DefaultJsonMapper : ObjectMapper() {

    init {
        registerModule(kotlinModule())
        registerModule(Jdk8Module())
        registerModule(JavaTimeModule())
        registerModule(BeanIntrospectionModule())

        // Serialization configuration.
        disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)

        // Deserialization configuration.
        enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
        disable(DeserializationFeature.READ_DATE_TIMESTAMPS_AS_NANOSECONDS)
        disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        disable(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES)

        // Collections and maps are included in the JSON payload even if empty to avoid deserialization issues.
        setSerializationInclusion(JsonInclude.Include.ALWAYS)
            .setDefaultPropertyInclusion(
                JsonInclude.Value.construct(JsonInclude.Include.ALWAYS, JsonInclude.Include.ALWAYS)
            )

        setVisibility(
            serializationConfig.defaultVisibilityChecker
                .withFieldVisibility(JsonAutoDetect.Visibility.ANY)
                .withGetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withSetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withCreatorVisibility(JsonAutoDetect.Visibility.PUBLIC_ONLY)
        )
    }
}