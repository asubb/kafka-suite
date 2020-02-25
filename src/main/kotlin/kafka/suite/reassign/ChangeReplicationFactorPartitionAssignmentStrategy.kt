package kafka.suite.reassign

import kafka.suite.KafkaBroker
import kafka.suite.KafkaPartitionAssignment
import mu.KotlinLogging

class ChangeReplicationFactorPartitionAssignmentStrategy(
        plan: KafkaPartitionAssignment,
        brokers: List<KafkaBroker>,
        weightFn: WeightFn,
        avoidBrokers: Set<Int>,
        private val isrBased: Boolean,
        private val replicationFactor: Int,
        private val skipNoLeader: Boolean = false
) : PartitionAssignmentStrategy(brokers, avoidBrokers, plan, weightFn) {

    private val logger = KotlinLogging.logger {}

    override fun newPlan(topics: Set<String>): KafkaPartitionAssignment {
        logger.debug {
            """
            ChangeReplicationFactorPartitionAssignmentStrategy(
                isrBased=$isrBased
                replicationFactor=$replicationFactor
                brokers=$brokers
            )
        """.trimIndent()
        }

        val nodesByRack = brokers.groupBy { it.rack }

        val newPartitions = plan.partitions.filter { it.topic in topics || topics.isEmpty() }.map { p ->
            logger.debug { "partition=$p" }
            val currentReplicas = if (isrBased) p.inSyncReplicas else p.replicas
            logger.debug { "currentReplicas=$currentReplicas" }
            val currentReplicationFactor = currentReplicas.count()
            logger.debug { "currentReplicationFactor=$currentReplicationFactor" }

            when {
                p.inSyncReplicas.size > replicationFactor || p.replicas.size > replicationFactor -> {
                    val toRemove = currentReplicationFactor - replicationFactor
                    logger.debug { "Decreasing RF by $toRemove" }

                    if (p.leader == null) {
                        if (skipNoLeader) {
                            return@map null
                        } else {
                            throw IllegalStateException("Can't decrease replication factor. You can skip such partitions. Partition=$p")
                        }
                    }

                    // if a few replicas on one rack we'll remove them first
                    val mostLoadedRackOrAllBrokers = currentReplicas.asSequence()
                            .map { b -> brokers.first { it.id == b } }
                            .groupBy { it.rack }
                            .map { it.key to it.value }
                            .groupBy { it.second.count() }
                            .maxBy { it.key }!!
                            .let { it.value.flatMap { it.second } }
                    // need to remove some of the nodes
                    val newReplicas = currentReplicas.toMutableList()
                    repeat(toRemove) {
                        val brokerToRemove = brokerLoadTracker.unselectNode(
                                mostLoadedRackOrAllBrokers.filter { it.id in newReplicas },
                                p
                        )
                        logger.debug { "brokerToRemove=$brokerToRemove" }
                        newReplicas -= brokerToRemove.id
                    }
                    p.copy(replicas = newReplicas)
                }
                p.inSyncReplicas.size < replicationFactor || p.replicas.size < replicationFactor -> {
                    logger.debug { "Increasing RF by ${replicationFactor - currentReplicationFactor}" }

                    val coveredRacks = currentReplicas
                            .map { b -> brokers.first { it.id == b } }
                            .map { it.rack }
                            .toSet()
                    val uncoveredRacks = (nodesByRack.keys - coveredRacks).toMutableSet()

                    val newReplicas = currentReplicas.toMutableList()

                    repeat(replicationFactor - currentReplicationFactor) {

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
                else -> {
                    logger.debug { "Leaving RF as is" }
                    null
                }
            }
        }

        return KafkaPartitionAssignment(plan.version, newPartitions.filterNotNull())
    }

}