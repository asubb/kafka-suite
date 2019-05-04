package kafka.suite.client

import kafka.suite.KafkaBroker
import kafka.suite.KafkaPartitionAssignment
import kafka.suite.Partition

interface KafkaAdminClient {
    fun topics(limitToTopics: Set<String> = emptySet()): Map<String, List<Partition>>
    fun brokers(): Map<Int, KafkaBroker>
    fun currentAssignment(limitToTopics: Set<String> = emptySet(), version: Int = 1): KafkaPartitionAssignment
    fun reassignPartitions(plan: KafkaPartitionAssignment): Boolean
    fun isReassignmentFinished(plan: KafkaPartitionAssignment): Boolean
}