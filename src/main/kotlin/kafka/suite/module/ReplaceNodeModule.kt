package kafka.suite.module

import kafka.suite.KafkaAdminClient
import kafka.suite.get
import kafka.suite.of
import kafka.suite.required
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options

class ReplaceNodeModule : RunnableModule {

    override fun module(): Module = Module.REPLACE_NODE

    private val r = Option("r", "replacing", true, "Kafka node ID to replace.").required()
    private val s = Option("s", "substitution", true, "Substitution kafka node ID.").required()
    private val t = Option("t", "topics", true, "Comma-separated list of topics to include, if not specified all topics will be included.")

    override fun getOptions(): Options = Options().of(r, s, t)

    override fun run(cli: CommandLine, kafkaAdminClient: KafkaAdminClient, dryRun: Boolean, waitToFinish: Boolean) {
        val replacing = cli.get(r) { it.first().toString().toInt() }
        val substitution = cli.get(s) { it.first().toString().toInt() }
        val topics = cli.get(t, emptySet()) { it.first().toString().split(",").toSet() }

        val brokers = kafkaAdminClient.brokers()
        println("Moving all partition for ${if (topics.isEmpty()) "all" else topics.toString()} from " +
                "node $replacing [${brokers.getValue(replacing).address}]  to node $substitution [${brokers.getValue(substitution).address}]")

        val strategy = ReplaceNodePartitionAssignmentStrategy(kafkaAdminClient, replacing, substitution, topics)
        val newPlan = strategy.newPlan()

        println("New assigment plan: $newPlan")

        if (!dryRun) {
            if (!kafkaAdminClient.reassignPartitions(newPlan)) {
                println("ERROR: Can't reassign partitions")
                return
            }

            if (waitToFinish) {
                val start = System.currentTimeMillis()
                while (!kafkaAdminClient.isReassignmentFinished(newPlan)) {
                    println("Elapsed ${(System.currentTimeMillis() - start) / 1000.0f}s")
                    Thread.sleep(10000)
                }
            }
        }
    }
}