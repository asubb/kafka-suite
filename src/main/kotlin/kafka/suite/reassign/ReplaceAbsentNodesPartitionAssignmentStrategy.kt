package kafka.suite.reassign

import kafka.suite.KafkaBroker
import kafka.suite.KafkaPartitionAssignment
import kafka.suite.Partition
import mu.KotlinLogging

class ReplaceAbsentNodesPartitionAssignmentStrategy(
        private val plan: KafkaPartitionAssignment,
        private val brokers: List<KafkaBroker>,
        private val weightFn: WeightFn
) : PartitionAssignmentStrategy {

    private val logger = KotlinLogging.logger {}

    override fun newPlan(): KafkaPartitionAssignment {

        val brokerLoadTracker = BrokerLoadTracker(brokers, plan, weightFn)

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
                        .filter { it.inSyncReplicas.size < replicationFactors.getValue(it.topic) }
                        .map { p ->
                            logger.debug { "Partition $p" }
                            try {
                                val inSyncReplicas = p.inSyncReplicas
                                        .filter { getRackForNode(it) != null } // filter out dead ISRs which may still be there

                                val brokersLeft = brokers
                                        .map { it.id }
                                        .filter { it !in inSyncReplicas }
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
                                logger.debug { "coveredRacks=$coveredRacks" }
                                val uncoveredRacks = racks - coveredRacks
                                logger.debug { "uncoveredRacks=$uncoveredRacks" }

                                // if there are uncovered racks spotted, select node from it first, should be enough in case if replication factor <= number of racks
                                val coverMoreRacks = uncoveredRacks.map { rack ->
                                    bookNode(nodesByRack.getValue(rack), p)
                                }
                                logger.debug { "coverMoreRacks=$coverMoreRacks" }

                                // if the partition still didn't find home, i.e. if replication factor > number of racks, it needs to choose some other node from the cluster.
                                val nonCoveredReplicationFactor = replicationFactors.getValue(p.topic) - (coverMoreRacks.size + coveredRacks.size)
                                logger.debug { "nonCoveredReplicationFactor=$nonCoveredReplicationFactor" }
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