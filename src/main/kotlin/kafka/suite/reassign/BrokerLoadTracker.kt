package kafka.suite.reassign

import kafka.suite.KafkaBroker
import kafka.suite.KafkaPartitionAssignment
import kafka.suite.Partition
import mu.KotlinLogging

class BrokerLoadTracker(
        brokers: List<KafkaBroker>,
        private val plan: KafkaPartitionAssignment,
        private val weightFn: WeightFn
) {

    private val logger = KotlinLogging.logger {}

    private val currentBrokerLoad: MutableMap<KafkaBroker, Int>

    init {
        currentBrokerLoad = mutableMapOf(*brokers
                .map { broker ->
                    broker to
                            plan.partitions
                                    .filter { broker.id in it.replicas }
                                    .map { weightFn.weight(it) }
                                    .sum()
                }.toTypedArray()
        )
        logger.debug { "currentBrokerLoad=$currentBrokerLoad" }

    }

    fun selectNode(nodes: Collection<KafkaBroker>, forPartition: Partition): KafkaBroker {
        val brokerAndPartition = sortNodes(nodes, forPartition)
                .first()// book the least loaded node

        val broker = brokerAndPartition.first
        val currentLoad = currentBrokerLoad.getValue(broker)
        currentBrokerLoad[broker] = currentLoad + weightFn.weight(forPartition)
        return broker
    }

    fun unselectNode(nodes: Collection<KafkaBroker>, forPartition: Partition): KafkaBroker {
        val brokerAndPartition = sortNodes(nodes, forPartition)
                .last() // free up the most loaded node

        val broker = brokerAndPartition.first
        val currentLoad = currentBrokerLoad.getValue(broker)
        currentBrokerLoad[broker] = currentLoad - weightFn.weight(forPartition)
        return broker
    }

    private fun sortNodes(nodes: Collection<KafkaBroker>, forPartition: Partition): List<Pair<KafkaBroker, Partition>> {
        return nodes
                .zip((0 until nodes.size).map { forPartition })
                .sortedWith(weightFn.comparator())
    }

}
