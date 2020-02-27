package kafka.suite

import kafka.suite.client.KafkaAdminClient
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options

class AnalyzeModule : RunnableModule {

    private val t = Option("t", "topics", true, "Comma-separated list of topics to include, if not specified all topics will be included.")

    override fun getDescription(): String = "Goes through the brokers trying to analyze what needs to be fixed."

    override fun module(): Module = Module.ANALYZE

    override fun getOptions(): Options = Options().of(t)

    override fun run(cli: CommandLine, kafkaAdminClient: KafkaAdminClient, dryRun: Boolean) {
        val limitToTopics = cli.get(t) { it.split(",").toSet() } ?: emptySet()
        val assignment = kafkaAdminClient.currentAssignment(limitToTopics)
        val brokerIds = kafkaAdminClient.brokers().keys

        val c = assignment.partitions
                .mapNotNull { p ->
                    when {
                        p.leader == null -> p to "NO LEADER"
                        p.inSyncReplicas.size != p.replicas.size -> p to "UNDER REPLICATED"
                        p.inSyncReplicas.sorted() != p.replicas.sorted() -> p to "REBALANCING"
                        !p.replicas.all { it in brokerIds } || !p.inSyncReplicas.all { it in brokerIds } -> p to "ABSENT BROKER"
                        else -> null
                    }
                }
                .groupBy { it.first.topic }
                .map {
                    println("Topic: ${it.key}")
                    it.value.forEach { (partition, problem) ->
                        println(String.format("%3s %20s <ISR:%s, Replicas: %s, Leader: %s>",
                                partition.partition,
                                problem,
                                partition.inSyncReplicas,
                                partition.replicas,
                                partition.leader ?: "none"
                        ))
                    }
                }
                .count()
        if (c == 0)
            println("Everything is fine.")
        else
            println("Total problems: $c")
    }
}