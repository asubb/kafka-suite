package kafka.suite.reassign

import kafka.suite.*
import kafka.suite.client.KafkaAdminClient
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import java.util.*

class FixNoLeaderModule : BaseReassignmentModule() {
    override fun getDescription(): String =
            "Fixes no leader on under replicated partition by assigning it to a different broker keeping the same replication factor or overriding with another one"


    private val t = Option("t", "topics", true, "Comma-separated list of topics to include, if not specified all topics will be included.")
    private val r = Option("r", "replication-factor", true, "Replication factor to set.")

    override fun module(): Module = Module.FIX_NO_LEADER

    override fun getOptions(): Options = Options().of(t, r)

    override fun getStrategy(cli: CommandLine, kafkaAdminClient: KafkaAdminClient): PartitionAssignmentStrategy {
        val topics = cli.get(t) { it.first().toString().split(",").toSet() } ?: emptySet()
        val replicationFactor = cli.get(r) { it.first().toString().toInt() }

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
                kafkaAdminClient,
                topics,
                brokers.values.toList(),
                weightFn,
                sortFn,
                replicationFactor
        )
    }

}