package kafka.suite.reassign

import kafka.suite.KafkaBroker
import kafka.suite.KafkaPartitionAssignment

class ReplaceNodePartitionAssignmentStrategy(
        broker: List<KafkaBroker>,
        plan: KafkaPartitionAssignment,
        weightFn: WeightFn,
        avoidBrokers: Set<Int>,
        private val nodeToReplace: KafkaBroker,
        private val substitutionNode: KafkaBroker
) : PartitionAssignmentStrategy(broker, avoidBrokers, plan, weightFn) {

    override fun newPlan(topics: Set<String>): KafkaPartitionAssignment {
        return KafkaPartitionAssignment(
                plan.version,
                plan.partitions
                        .filter { it.topic in topics || topics.isEmpty() }
                        .map { p ->
                            brokerLoadTracker.selectNodeManually(substitutionNode, p)
                            brokerLoadTracker.unselectNodeManually(nodeToReplace, p)
                            p.copy(
                                    replicas = p.replicas.map { if (it == nodeToReplace.id) substitutionNode.id else it }
                            )
                        }
                        .toList()
        )
    }

}