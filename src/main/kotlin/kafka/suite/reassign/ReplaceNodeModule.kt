package kafka.suite.reassign

import kafka.suite.*
import kafka.suite.client.KafkaAdminClient
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options

class ReplaceNodeModule : BaseReassignmentModule() {
    override fun getDescription(): String =
            "Moves partitions from one broker to another. Broker should be available"

    private val r = Option("r", "replacing", true, "Kafka node ID to replace.").required()
    private val s = Option("s", "substitution", true, "Substitution kafka node ID.").required()

    override fun module(): Module = Module.REPLACE_NODE

    override fun getOptionList(): List<Option> = listOf(r, s)

    override fun getStrategy(cli: CommandLine, kafkaAdminClient: KafkaAdminClient, plan: KafkaPartitionAssignment): PartitionAssignmentStrategy {

        val brokers = kafkaAdminClient.brokers()

        val replacing = cli.getRequired(r) { brokers.getValue(it.toInt()) }
        val substitution = cli.getRequired(s) { brokers.getValue(it.toInt()) }


        return ReplaceNodePartitionAssignmentStrategy(
                plan,
                replacing,
                substitution
        )
    }

}