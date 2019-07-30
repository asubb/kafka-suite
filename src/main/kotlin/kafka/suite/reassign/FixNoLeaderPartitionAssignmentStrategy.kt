package kafka.suite.reassign

import kafka.suite.KafkaBroker
import kafka.suite.KafkaPartitionAssignment
import kafka.suite.Partition
import kafka.suite.client.KafkaAdminClient
import mu.KotlinLogging

class FixNoLeaderPartitionAssignmentStrategy(
        private val client: KafkaAdminClient,
        private val limitToTopics: Set<String>,
        private val brokers: List<KafkaBroker>,
        private val weightFn: (Partition) -> Int,
        private val sortFn: Comparator<Pair<KafkaBroker, Partition>>,
        private val newReplicationFactor: Int? = null
) : PartitionAssignmentStrategy {

    private val logger = KotlinLogging.logger {}

    override fun newPlan(): KafkaPartitionAssignment {
        logger.debug {
            """
            FixNoLeaderPartitionAssignmentStrategy(
                limitToTopics=$limitToTopics
                brokers=$brokers
            )
        """.trimIndent()
        }

        val plan = client.currentAssignment(limitToTopics)
        logger.debug { "currentAssignment=$plan" }

        val brokerLoadTracker = BrokerLoadTracker(brokers, plan, weightFn, sortFn)

        val nodesByRack = brokers.groupBy { it.rack }

        val newPartitions = plan.partitions
                .filter { it.leader == null }
                .map { p ->
                    val replicationFactor = newReplicationFactor ?: p.replicas.size
                    logger.debug { "Aimed for replication factor $replicationFactor" }

                    // no new covered racks and replicas, need to be done from scratch
                    val uncoveredRacks = nodesByRack.keys.toMutableSet()
                    val newReplicas = ArrayList<Int>()

                    repeat(replicationFactor) {

                        val candidates =
                                if (uncoveredRacks.isEmpty())
                                    brokers
                                else
                                    uncoveredRacks.map { nodesByRack.getValue(it) }.flatten()

                        val brokerToAdd = brokerLoadTracker.selectNode(
                                candidates.filter {
                                    it.id !in newReplicas // filter out existing replicas
                                },
                                p
                        )
                        logger.debug { "brokerToAdd=$brokerToAdd" }
                        newReplicas += brokerToAdd.id
                        uncoveredRacks -= brokerToAdd.rack
                    }

                    p.copy(replicas = newReplicas)
                }

        return KafkaPartitionAssignment(plan.version, newPartitions)
    }

}