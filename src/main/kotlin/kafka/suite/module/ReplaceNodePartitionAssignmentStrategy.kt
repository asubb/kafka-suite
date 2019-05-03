package kafka.suite.module

import kafka.suite.KafkaAdminClient
import kafka.suite.KafkaPartitionAssignment

class ReplaceNodePartitionAssignmentStrategy(
        private val client: KafkaAdminClient,
        private val nodeToReplace: Int,
        private val substitutionNode: Int,
        private val limitToTopics: Set<String>
) : PartitionAssignmentStrategy {
    override fun newPlan(): KafkaPartitionAssignment {
        val plan = client.currentAssignment()
        return KafkaPartitionAssignment(
                plan.version,
                plan.partitions
                        .filter { limitToTopics.isEmpty() || it.topic in limitToTopics }
                        .map { p ->
                            p.copy(
                                    replicas = p.replicas.map { if (it == nodeToReplace) substitutionNode else it }
                            )
                        }
                        .toList()
        )
    }

}