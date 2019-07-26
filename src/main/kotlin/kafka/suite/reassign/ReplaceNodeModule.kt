package kafka.suite.reassign

import kafka.suite.*
import kafka.suite.client.KafkaAdminClient
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options

class ReplaceNodeModule : BaseReassignmentModule() {
    override fun getDescription(): String =
            "moves partitions from one node to another"

    private val r = Option("r", "replacing", true, "Kafka node ID to replace.").required()
    private val s = Option("s", "substitution", true, "Substitution kafka node ID.").required()
    private val t = Option("t", "topics", true, "Comma-separated list of topics to include, if not specified all topics will be included.")

    override fun module(): Module = Module.REPLACE_NODE

    override fun getOptions(): Options = Options().of(r, s, t)

    override fun getStrategy(cli: CommandLine, kafkaAdminClient: KafkaAdminClient): PartitionAssignmentStrategy {

        val replacing = cli.get(r) { it.first().toString().toInt() }
        val substitution = cli.get(s) { it.first().toString().toInt() }
        val topics = cli.get(t, emptySet()) { it.first().toString().split(",").toSet() }

        val brokers = kafkaAdminClient.brokers()
        println("Moving all partitions for ${if (topics.isEmpty()) "all topics" else "topics $topics"} from " +
                "node $replacing [${brokers[replacing]?.address
                        ?: "unregistered"}]  to node $substitution [${brokers.getValue(substitution).address}]")

        return ReplaceNodePartitionAssignmentStrategy(kafkaAdminClient, replacing, substitution, topics)
    }

}