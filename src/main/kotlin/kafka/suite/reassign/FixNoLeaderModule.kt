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

    override fun getStrategy(cli: CommandLine, kafkaAdminClient: KafkaAdminClient, plan: KafkaPartitionAssignment, weightFn: WeightFn): PartitionAssignmentStrategy {
        val replicationFactor = cli.get(r) { it.toInt() }

        val brokers = kafkaAdminClient.brokers()

        return FixNoLeaderPartitionAssignmentStrategy(
                plan,
                brokers.values.toList(),
                weightFn,
                replicationFactor
        )
    }

}