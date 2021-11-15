package io.qalipsis.plugins.kafka

import io.qalipsis.api.scenario.ScenarioSpecification
import io.qalipsis.api.steps.AbstractPluginStepWrapper
import io.qalipsis.api.steps.AbstractScenarioSpecificationWrapper
import io.qalipsis.api.steps.StepSpecification


/**
 * Interface of a Kafka step to define it in the appropriate step specifications namespace.
 *
 * @author Eric Jessé
 */
interface KafkaStepSpecification<INPUT, OUTPUT, SELF : StepSpecification<INPUT, OUTPUT, SELF>> :
    StepSpecification<INPUT, OUTPUT, SELF>

/**
 * Step wrapper to enter the namespace for the Kafka step specifications.
 *
 * @author Eric Jessé
 */
class KafkaStepSpecificationImpl<INPUT, OUTPUT>(wrappedStepSpec: StepSpecification<INPUT, OUTPUT, *>) :
    AbstractPluginStepWrapper<INPUT, OUTPUT>(wrappedStepSpec),
    KafkaStepSpecification<INPUT, OUTPUT, AbstractPluginStepWrapper<INPUT, OUTPUT>>

fun <INPUT, OUTPUT> StepSpecification<INPUT, OUTPUT, *>.kafka(): KafkaStepSpecification<INPUT, OUTPUT, *> =
    KafkaStepSpecificationImpl(this)

/**
 * Scenario wrapper to enter the namespace for the Kafka step specifications.
 *
 * You can learn more about Apache Kafka on [the official website](https://kafka.apache.org).
 *
 * @author Eric Jessé
 */
class KafkaScenarioSpecification(scenario: ScenarioSpecification) :
    AbstractScenarioSpecificationWrapper(scenario)

fun ScenarioSpecification.kafka() = KafkaScenarioSpecification(this)