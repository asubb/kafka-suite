package kafka.suite.reassign

import kafka.suite.*
import kafka.suite.client.KafkaAdminClient
import java.lang.IllegalStateException

class ReplaceAbsentNodesPartitionAssignmentStrategy(
        private val client: KafkaAdminClient,
        private val limitToTopics: Set<String>,
        private val brokers: List<KafkaBroker>,
        private val weightFn: (Partition) -> Int,
        private val sortFn: Comparator<Pair<KafkaBroker, Partition>>
) : PartitionAssignmentStrategy {


    override fun newPlan(): KafkaPartitionAssignment {
        val plan = client.currentAssignment(limitToTopics)

        val currentBrokerLoad = mutableMapOf(*brokers
                .map { broker ->
                    broker to
                            plan.partitions
                                    .filter { broker.id in it.replicas }
                                    .map(weightFn)
                                    .sum()
                }.toTypedArray()
        )


        val nodesByRack = brokers.groupBy { it.rack }
        val racks = brokers.map { it.rack }.toSet()
        val replicationFactors = plan.partitions.asSequence()
                .groupBy { it.topic }
                .filter { limitToTopics.isEmpty() || it.key in limitToTopics }
                .map { it.key to it.value.maxBy { p -> p.replicas.size }!!.replicas.size }
                .toMap()

        fun selectNode(nodes: Collection<KafkaBroker>, forPartition: Partition): KafkaBroker {
            val brokerAndPartition = nodes
                    .zip((0 until nodes.size).map { forPartition })
                    .sortedWith(sortFn)
                    .first()

            val broker = brokerAndPartition.first
            val currentLoad = currentBrokerLoad.getValue(broker)
            currentBrokerLoad[broker] = currentLoad + weightFn(forPartition)
            return broker
        }

        return KafkaPartitionAssignment(
                plan.version,
                plan.partitions
                        .filter { limitToTopics.isEmpty() || it.topic in limitToTopics }
                        .filter { it.inSyncReplicas.size < replicationFactors.getValue(it.topic) }
                        .map { p ->
                            val brokersLeft = brokers
                                    .map { it.id }
                                    .filter { it !in p.inSyncReplicas }
                                    .toMutableSet()

                            fun bookNode(brokers: List<KafkaBroker>, forPartition: Partition): KafkaBroker {
                                if (brokersLeft.isEmpty()) throw IllegalStateException("No enough brokers to fulfil the replication factor")
                                val broker = selectNode(brokers.filter { it.id in brokersLeft }, forPartition)
                                brokersLeft -= broker.id
                                return broker
                            }

                            // these are the racks which is already covered, try to find the rest to cover, as each rack should have contain at least one replica
                            val coveredRacks = p.inSyncReplicas
                                    .map { r -> brokers.filter { it.id == r }.map { it.rack }.first() }
                                    .toSet()
                            val uncoveredRacks = racks - coveredRacks

                            // if there are uncovered racks spotted, select node from it first, should be enough in case if replication factor <= number of racks
                            val coverMoreRacks = uncoveredRacks.map { rack ->
                                bookNode(nodesByRack.getValue(rack), p)
                            }

                            // if the partition still didn't find home, i.e. if replication factor > number of racks, it needs to choose some other node from the cluster.
                            val nonCoveredReplicationFactor = replicationFactors.getValue(p.topic) - (coverMoreRacks.size + coveredRacks.size)
                            val spreadTheRest = (0 until nonCoveredReplicationFactor)
                                    .map { bookNode(brokers, p) }

                            p.copy(
                                    replicas = p.inSyncReplicas + (coverMoreRacks + spreadTheRest).map { it.id }
                            )
                        }
                        .toList()
        )
    }


}