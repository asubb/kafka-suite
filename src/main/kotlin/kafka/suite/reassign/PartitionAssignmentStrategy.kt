package kafka.suite.reassign

import kafka.suite.KafkaBroker
import kafka.suite.KafkaPartitionAssignment

abstract class PartitionAssignmentStrategy(
        val brokers: List<KafkaBroker>,
        val avoidBrokers: Set<Int>,
        val plan: KafkaPartitionAssignment,
        val weightFn: WeightFn
) {

    val brokerLoadTracker = BrokerLoadTracker(brokers, plan, weightFn, avoidBrokers)

    abstract fun newPlan(topics: Set<String> = emptySet()): KafkaPartitionAssignment
}

