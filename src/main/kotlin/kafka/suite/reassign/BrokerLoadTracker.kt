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

    private val currentBrokerLoad: MutableMap<KafkaBroker, Long>

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

    private val comparator = Comparator { o1: Pair<KafkaBroker, Partition>, o2: Pair<KafkaBroker, Partition> ->
        val b1Load = currentBrokerLoad.getValue(o1.first)
        val b2Load = currentBrokerLoad.getValue(o2.first)

        val d = b1Load + weightFn.weight(o1.second) - (b2Load + weightFn.weight(o2.second))

        when {
            d < 0 -> -1
            d > 0 -> 1
            else -> 0
        }
    }

    fun getLoad(): Map<KafkaBroker, Long> = currentBrokerLoad

    fun selectNodeManually(broker: KafkaBroker, forPartition: Partition) {
        val currentLoad = currentBrokerLoad.getValue(broker)
        currentBrokerLoad[broker] = currentLoad + weightFn.weight(forPartition)
    }

    fun unselectNodeManually(broker: KafkaBroker, forPartition: Partition) {
        val currentLoad = currentBrokerLoad.getValue(broker)
        currentBrokerLoad[broker] = currentLoad - weightFn.weight(forPartition)
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
                .sortedWith(comparator)
    }

}
