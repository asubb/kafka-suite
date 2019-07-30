package kafka.suite.reassign

import kafka.suite.*
import kafka.suite.client.KafkaAdminClient
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.Option
import java.util.*

class FixNoLeaderModule : BaseReassignmentModule() {

    override fun getDescription(): String =
            "Fixes no leader on under replicated partition by assigning it to a different broker keeping the same replication factor or overriding with another one"

    private val r = Option("r", "replication-factor", true, "Replication factor to set.")

    override fun module(): Module = Module.FIX_NO_LEADER

    override fun getOptionList(): List<Option> = listOf(r)

    override fun getStrategy(cli: CommandLine, kafkaAdminClient: KafkaAdminClient, plan: KafkaPartitionAssignment): PartitionAssignmentStrategy {
        val replicationFactor = cli.get(r) { it.toInt() }

        val brokers = kafkaAdminClient.brokers()

        val weightFn: (Partition) -> Int = { 1 } // TODO collect/load/generate stats

        val sortFn = object : Comparator<Pair<KafkaBroker, Partition>> {

            private val r = Random() // TODO that should be provided as a parameter, and overall that should be better thought

            override fun compare(o1: Pair<KafkaBroker, Partition>, o2: Pair<KafkaBroker, Partition>): Int {
                val nextInt = r.nextInt()
                return if (nextInt == 0 || nextInt < 0) -1 else 1
            }

        }

        return FixNoLeaderPartitionAssignmentStrategy(
                plan,
                brokers.values.toList(),
                weightFn,
                sortFn,
                replicationFactor
        )
    }

}