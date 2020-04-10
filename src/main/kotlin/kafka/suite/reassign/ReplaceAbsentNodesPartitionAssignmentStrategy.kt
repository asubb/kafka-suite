package kafka.suite.reassign

import kafka.suite.KafkaBroker
import kafka.suite.KafkaPartitionAssignment
import kafka.suite.Partition
import mu.KotlinLogging
import java.lang.Integer.min

class ReplaceAbsentNodesPartitionAssignmentStrategy(
        plan: KafkaPartitionAssignment,
        brokers: List<KafkaBroker>,
        weightFn: WeightFn,
        avoidBrokers: Set<Int>,
        private val maxReplicationFactor: Int = Int.MAX_VALUE,
        private val missingBrokers: Set<Int> = emptySet()
) : PartitionAssignmentStrategy(brokers, avoidBrokers, plan, weightFn) {

    private val logger = KotlinLogging.logger {}

    override fun newPlan(topics: Set<String>): KafkaPartitionAssignment {

        val nodesByRack = brokers.groupBy { it.rack }
        logger.debug { "nodesByRack=$nodesByRack" }
        val racks = brokers.map { it.rack }.toSet()
        logger.debug { "racks=$racks" }
        val replicationFactors = plan.partitions.asSequence()
                .groupBy { it.topic }
                .map { it.key to it.value.maxBy { p -> p.replicas.size }!!.replicas.size }
                .toMap()
        logger.debug { "replicationFactors=$replicationFactors" }

        return KafkaPartitionAssignment(
                plan.version,
                plan.partitions
                        .filter { it.topic in topics || topics.isEmpty() }
                        .filter { it.replicas.any { it in missingBrokers } || it.inSyncReplicas.size < replicationFactors.getValue(it.topic) }
                        .map { p ->
                            logger.debug { "Partition $p" }
                            try {
                                val inSyncReplicas = p.inSyncReplicas
                                        .filter { getRackForNode(it) != null } // filter out dead ISRs which may still be there
                                var nonCoveredReplicationFactor = min(replicationFactors.getValue(p.topic), maxReplicationFactor)
                                logger.debug {"nonCoveredReplicationFactor in the beginning: $nonCoveredReplicationFactor"}

                                val brokersLeft = brokers
                                        .map { it.id }
                                        .filter { it !in inSyncReplicas && it !in missingBrokers }
                                        .toMutableSet()
                                logger.debug { "Brokers Left $brokersLeft" }

                                fun bookNode(brokers: List<KafkaBroker>, forPartition: Partition): KafkaBroker {
                                    if (brokersLeft.isEmpty()) throw IllegalStateException("No enough brokers to fulfil the replication factor")
                                    val broker = brokerLoadTracker.selectNode(brokers.filter { it.id in brokersLeft }, forPartition)
                                    brokersLeft -= broker.id
                                    return broker
                                }

                                // these are the racks which is already covered, try to find the rest to cover, as each rack should have contain at least one replica
                                val coveredRacks = inSyncReplicas
                                        .map { r -> getRackForNode(r)!! }
                                        .toSet()
                                nonCoveredReplicationFactor -= coveredRacks.size
                                logger.debug {"nonCoveredReplicationFactor after checking ISR: $nonCoveredReplicationFactor"}
                                logger.debug { "coveredRacks=$coveredRacks" }
                                val uncoveredRacks = racks - coveredRacks
                                logger.debug { "uncoveredRacks=$uncoveredRacks" }


                                // if there are uncovered racks spotted, select node from it first, should be enough in case if replication factor <= number of racks
                                val coverMoreRacks = uncoveredRacks.map { rack ->
                                    bookNode(nodesByRack.getValue(rack), p)
                                }.take(nonCoveredReplicationFactor)
                                logger.debug { "coverMoreRacks=$coverMoreRacks" }
                                nonCoveredReplicationFactor -= coverMoreRacks.size
                                logger.debug {"After covering more racks $nonCoveredReplicationFactor"}

                                // if the partition still didn't find home, i.e. if replication factor > number of racks, it needs to choose some other node from the cluster.
                                logger.debug { "nonCoveredReplicationFactor before spreading the rest $nonCoveredReplicationFactor" }
                                val spreadTheRest = (0 until nonCoveredReplicationFactor)
                                        .map { bookNode(brokers, p) }
                                logger.debug { "spreadTheRest=$spreadTheRest" }

                                p.copy(
                                        replicas = inSyncReplicas + (coverMoreRacks + spreadTheRest).map { it.id }
                                )
                            } catch (e: Exception) {
                                println("Error with partition: $p")
                                throw e
                            }
                        }
                        .toList()
        )
    }

    private fun getRackForNode(r: Int): String? =
            brokers
                    .filter { it.id == r }
                    .map { it.rack }
                    .firstOrNull()


}