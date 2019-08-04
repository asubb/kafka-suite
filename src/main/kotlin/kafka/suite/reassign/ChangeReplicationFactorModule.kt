package kafka.suite.reassign

import kafka.suite.*
import kafka.suite.client.KafkaAdminClient
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.Option

class ChangeReplicationFactorModule : BaseReassignmentModule() {
    override fun getDescription(): String =
            "Changes replication factor of topics"

    private val r = Option("r", "replication-factor", true, "Replication factor to set.").required()
    private val i = Option("i", "isr-based", false, "Use in-sync replicas instead of replicas as a current state.")
    private val s = Option("s", "skip-no-leader", false, "If some partitions need to be skipped during decreasing replication factor, this flag should be specified")

    override fun module(): Module = Module.CHANGE_RF

    override fun getOptionList(): List<Option> = listOf(r, i, s)

    override fun getStrategy(cli: CommandLine, kafkaAdminClient: KafkaAdminClient, plan: KafkaPartitionAssignment, weightFn: WeightFn): PartitionAssignmentStrategy {
        val isrBased = cli.has(i)
        val skipNoLeader = cli.has(s)
        val replicationFactor = cli.getRequired(r) { it.toInt() }

        val brokers = kafkaAdminClient.brokers()

        return ChangeReplicationFactorPartitionAssignmentStrategy(
                plan,
                isrBased,
                replicationFactor,
                brokers.values.toList(),
                weightFn,
                skipNoLeader
        )
    }

}