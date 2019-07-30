package kafka.suite.reassign

import kafka.suite.*
import kafka.suite.client.KafkaAdminClient
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import java.util.*

class ChangeReplicationFactorModule : BaseReassignmentModule() {
    override fun getDescription(): String =
            "Changes replication factor of topics"


    private val t = Option("t", "topics", true, "Comma-separated list of topics to include, if not specified all topics will be included.")
    private val r = Option("r", "replication-factor", true, "Replication factor to set.").required()
    private val i = Option("i", "isr-based", false, "Use in-sync replicas instead of replicas as a current state.")
    private val s = Option("s", "skip-no-leader", false, "If some partitions need to be skipped during decreasing replication factor, this flag should be specified")

    override fun module(): Module = Module.CHANGE_RF

    override fun getOptions(): Options = Options().of(t, r, i, s)

    override fun getStrategy(cli: CommandLine, kafkaAdminClient: KafkaAdminClient): PartitionAssignmentStrategy {
        val topics = cli.get(t) { it.first().toString().split(",").toSet() } ?: emptySet()
        val isrBased = cli.has(i)
        val skipNoLeader = cli.has(s)
        val replicationFactor = cli.getRequired(r) { it.first().toString().toInt() }

        val brokers = kafkaAdminClient.brokers()

        val weightFn: (Partition) -> Int = { 1 } // TODO collect/load/generate stats

        val sortFn = object : Comparator<Pair<KafkaBroker, Partition>> {

            private val r = Random() // TODO that should be provided as a parameter, and overall that should be better thought

            override fun compare(o1: Pair<KafkaBroker, Partition>, o2: Pair<KafkaBroker, Partition>): Int {
                val nextInt = r.nextInt()
                return if (nextInt == 0 || nextInt < 0) -1 else 1
            }

        }

        return ChangeReplicationFactorPartitionAssignmentStrategy(
                isrBased,
                replicationFactor,
                kafkaAdminClient,
                topics,
                brokers.values.toList(),
                weightFn,
                sortFn,
                skipNoLeader
        )
    }

}