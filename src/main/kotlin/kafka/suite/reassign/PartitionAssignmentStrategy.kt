package kafka.suite.reassign

import kafka.suite.KafkaPartitionAssignment

interface PartitionAssignmentStrategy {
    fun newPlan(): KafkaPartitionAssignment
}

