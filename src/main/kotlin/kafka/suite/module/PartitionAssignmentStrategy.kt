package kafka.suite.module

import kafka.suite.KafkaPartitionAssignment

interface PartitionAssignmentStrategy {
    fun newPlan(): KafkaPartitionAssignment
}

