package kafka.suite

import kafka.suite.KafkaPartitionAssignment

interface PartitionAssignmentStrategy {
    fun newPlan(): KafkaPartitionAssignment
}

