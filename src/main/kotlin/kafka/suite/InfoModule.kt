package kafka.suite

import kafka.suite.client.KafkaAdminClient
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options

class InfoModule : RunnableModule {

    private val t = Option("t", "topics", true, "Comma-separated list of topics to include, if not specified all topics will be included.")

    override fun module(): Module = Module.INFO

    override fun getOptions(): Options = Options().of(t)

    override fun getDescription(): String = "Gets info about cluster"

    override fun run(cli: CommandLine, kafkaAdminClient: KafkaAdminClient, dryRun: Boolean, waitToFinish: Boolean) {
        val topics = cli.get(t, emptySet()) { it.first().toString().split(",").toSet() }

        val brokers = kafkaAdminClient.brokers()
        val assignment = kafkaAdminClient.currentAssignment(topics)

        println("""
            Brokers [${brokers.size}]: $brokers
            
            Assignment: $assignment
        """.trimIndent())
    }
}