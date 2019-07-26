package kafka.suite

import kafka.suite.client.KafkaAdminClient
import java.lang.UnsupportedOperationException

class TestKafkaAdminClient(
        val topicsFn: (limitToTopics: Set<String>) -> Map<String, List<Partition>> = { _ -> throw UnsupportedOperationException() },
        val brokersFn: () -> Map<Int, KafkaBroker> = { throw UnsupportedOperationException() },
        val currentAssignmentFn: (limitToTopics: Set<String>, version: Int) -> KafkaPartitionAssignment = { _, _ -> throw UnsupportedOperationException() },
        val reassignPartitionsFn: (plan: KafkaPartitionAssignment) -> Boolean = { _ -> throw UnsupportedOperationException() },
        val isReassignmentFinishedFn: (plan: KafkaPartitionAssignment) -> Boolean = { _ -> throw UnsupportedOperationException() }
) : KafkaAdminClient {
    override fun topics(limitToTopics: Set<String>): Map<String, List<Partition>> = topicsFn(limitToTopics)

    override fun brokers(): Map<Int, KafkaBroker> = brokersFn()

    override fun currentAssignment(limitToTopics: Set<String>, version: Int): KafkaPartitionAssignment = currentAssignmentFn(limitToTopics, version)

    override fun reassignPartitions(plan: KafkaPartitionAssignment): Boolean = reassignPartitionsFn(plan)

    override fun isReassignmentFinished(plan: KafkaPartitionAssignment): Boolean = isReassignmentFinishedFn(plan)

}