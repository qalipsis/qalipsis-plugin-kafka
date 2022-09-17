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

/**
 * Records the metrics for the Kafka consumer.
 *
 * @property keysBytesReceived records the number of bytes sent for the serialized keys.
 * @property valuesBytesReceived records the number of bytes sent for the serialized values.
 * @property recordsCount counts the number of records sent.
 *
 * @author Alex Averyanov
 */
data class KafkaConsumerMeters(
    var keysBytesReceived: Int = 0,
    var valuesBytesReceived: Int = 0,
    var recordsCount: Int = 0
)