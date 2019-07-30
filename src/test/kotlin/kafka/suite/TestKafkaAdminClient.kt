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

    override fun updatePartitionAssignment(topic: String, partition: Int, replicas: List<Int>, leader: Int) = throw UnsupportedOperationException()

    override fun currentReassignment(): KafkaPartitionAssignment? = throw UnsupportedOperationException()

    override fun topics(limitToTopics: Set<String>): Map<String, List<Partition>> = topicsFn(limitToTopics)

    override fun brokers(): Map<Int, KafkaBroker> = brokersFn()

    override fun currentAssignment(limitToTopics: Set<String>, version: Int): KafkaPartitionAssignment = currentAssignmentFn(limitToTopics, version)

    override fun reassignPartitions(plan: KafkaPartitionAssignment): Boolean = reassignPartitionsFn(plan)

    override fun isReassignmentFinished(plan: KafkaPartitionAssignment): Boolean = isReassignmentFinishedFn(plan)

}

fun brokers(brokersCount: Int, racksCount: Int, start: Int = 1): List<KafkaBroker> {
    require(racksCount > 0) { "racksCount > 0" }
    require(brokersCount > 0) { "brokersCount > 0" }
    return (start..start + brokersCount)
            .map { KafkaBroker(it, "127.0.0.$it", "${it % racksCount}") }

}
