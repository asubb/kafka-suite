package kafka.suite.reassign

import kafka.suite.KafkaBroker
import kafka.suite.KafkaPartitionAssignment
import mu.KotlinLogging

class FixNoLeaderPartitionAssignmentStrategy(
        plan: KafkaPartitionAssignment,
        brokers: List<KafkaBroker>,
        weightFn: WeightFn,
        avoidBrokers: Set<Int>,
        private val newReplicationFactor: Int? = null
) : PartitionAssignmentStrategy(brokers, avoidBrokers, plan, weightFn) {

    private val logger = KotlinLogging.logger {}

    override fun newPlan(topics: Set<String>): KafkaPartitionAssignment {
        logger.debug {
            """
            FixNoLeaderPartitionAssignmentStrategy(
                brokers=$brokers
            )
        """.trimIndent()
        }

        val nodesByRack = brokers.groupBy { it.rack }

        val newPartitions = plan.partitions
                .filter { it.topic in topics || topics.isEmpty() }
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