package kafka.suite.reassign

import kafka.suite.KafkaBroker
import kafka.suite.KafkaPartitionAssignment
import kafka.suite.client.KafkaAdminClient

class ReplaceNodePartitionAssignmentStrategy(
        private val plan: KafkaPartitionAssignment,
        private val nodeToReplace: KafkaBroker,
        private val substitutionNode: KafkaBroker
) : PartitionAssignmentStrategy {

    override fun newPlan(): KafkaPartitionAssignment {
        return KafkaPartitionAssignment(
                plan.version,
                plan.partitions
                        .map { p ->
                            p.copy(
                                    replicas = p.replicas.map { if (it == nodeToReplace.id) substitutionNode.id else it }
                            )
                        }
                        .toList()
        )
    }

}