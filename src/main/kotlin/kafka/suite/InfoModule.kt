package kafka.suite

import kafka.suite.client.KafkaAdminClient
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options

class InfoModule : RunnableModule {

    private val t = Option("t", "topics", true, "Comma-separated list of topics to get info for")
    private val a = Option("a", "all-topics", false, "Get info for all topics")
    private val b = Option("b", "brokers", false, "Get info about brokers")

    override fun module(): Module = Module.INFO

    override fun getOptions(): Options = Options().of(t, a, b)

    override fun getDescription(): String = "Gets info about cluster"

    override fun run(cli: CommandLine, kafkaAdminClient: KafkaAdminClient, dryRun: Boolean, waitToFinish: Boolean) {
        cli.ifHas(t) {
            val topics = cli.get(t, emptySet()) { it.first().toString().split(",").toSet() }
            val assignment = kafkaAdminClient.currentAssignment(topics)
            println("Assignment: $assignment")
        }

        cli.ifHas(a) {
            val assignment = kafkaAdminClient.currentAssignment()
            println("Assignment: $assignment")
        }

        cli.ifHas(b) {
            val brokers = kafkaAdminClient.brokers()

            println("Brokers [${brokers.size}]: $brokers")
        }
    }
}