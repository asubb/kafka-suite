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

    override fun module(): Module = Module.REPLACE_ABSENT_NODE

    override fun getOptionList(): List<Option> = emptyList()

    override fun getStrategy(cli: CommandLine, kafkaAdminClient: KafkaAdminClient, plan: KafkaPartitionAssignment): PartitionAssignmentStrategy {

        val brokers = kafkaAdminClient.brokers()

        val weightFn: (Partition) -> Int = { 1 } // TODO collect/load/generate stats

        val sortFn = object : Comparator<Pair<KafkaBroker, Partition>> {

            private val r = Random() // TODO that should be provided as a parameter, and overall that should be better thought

            override fun compare(o1: Pair<KafkaBroker, Partition>, o2: Pair<KafkaBroker, Partition>): Int {
                val nextInt = r.nextInt()
                return if (nextInt == 0 || nextInt < 0) -1 else 1
            }

        }

        return ReplaceAbsentNodesPartitionAssignmentStrategy(
                plan,
                brokers.values.toList(),
                weightFn,
                sortFn
        )
    }

}