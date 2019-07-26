package kafka.suite.reassign

import kafka.suite.*
import kafka.suite.client.KafkaAdminClient
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import java.util.*

class ReplaceAbsentNodeModule : BaseReassignmentModule() {
    override fun getDescription(): String =
            "find the new home for under-replicated partitions if the node(s) disappeared. Automatically detects the replication factor based on current state and missed node"


    private val r = Option("r", "racks", true, "Comma-separated list of broker-rack correspondence. If it can't be fetched by client. Example: `1040:AZ1,1039:AZ2`, if there is no racks, just specify nothing")
    private val t = Option("t", "topics", true, "Comma-separated list of topics to include, if not specified all topics will be included.")

    override fun module(): Module = Module.REPLACE_ABSENT_NODE

    override fun getOptions(): Options = Options().of(r, t)

    override fun getStrategy(cli: CommandLine, kafkaAdminClient: KafkaAdminClient): PartitionAssignmentStrategy {
        val topics = cli.get(t, emptySet()) { it.first().toString().split(",").toSet() }

        val brokers = kafkaAdminClient.brokers()

        val userDefinedBrokerRack = cli.get(r, emptyMap()) {
            it.toString().split(",").asSequence()
                    .map {
                        val (brokerId, rack) = it.split(":", limit = 2)
                        val key = brokerId.toInt()
                        key to brokers.getValue(key)
                                .copy(rack = rack)
                    }
                    .toMap()
        }

        val weightFn: (Partition) -> Int = { 1 } // TODO collect/load/generate stats

        val sortFn = object : Comparator<Pair<KafkaBroker, Partition>> {

            private val r = Random() // TODO that should be provided as a parameter, and overall that should be better thought

            override fun compare(o1: Pair<KafkaBroker, Partition>, o2: Pair<KafkaBroker, Partition>): Int {
                val nextInt = r.nextInt()
                return if (nextInt == 0 || nextInt < 0) -1 else 1
            }

        }

        val brokerInfo = brokers.map {
            userDefinedBrokerRack[it.key] ?: it.value
        }.toList()

        println("Current broker info: $brokerInfo")

        return ReplaceAbsentNodesPartitionAssignmentStrategy(
                kafkaAdminClient,
                topics,
                brokerInfo,
                weightFn,
                sortFn
        )
    }

}