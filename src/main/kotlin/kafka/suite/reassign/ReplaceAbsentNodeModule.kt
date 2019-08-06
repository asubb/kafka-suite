package kafka.suite.reassign

import kafka.suite.*
import kafka.suite.client.KafkaAdminClient
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.Option

class ReplaceAbsentNodeModule : BaseReassignmentModule() {
    override fun getDescription(): String =
            "find the new home for under-replicated partitions if the node(s) disappeared. Automatically detects the replication factor based on current state and missed node"

    override fun module(): Module = Module.REPLACE_ABSENT_NODE

    override fun getOptionList(): List<Option> = emptyList()

    override fun getStrategy(cli: CommandLine, kafkaAdminClient: KafkaAdminClient, plan: KafkaPartitionAssignment, weightFn: WeightFn): PartitionAssignmentStrategy {

        val brokers = kafkaAdminClient.brokers()

        return ReplaceAbsentNodesPartitionAssignmentStrategy(
                plan,
                brokers.values.toList(),
                weightFn
        )
    }

}