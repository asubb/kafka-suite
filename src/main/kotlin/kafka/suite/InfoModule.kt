package kafka.suite

import kafka.suite.client.KafkaAdminClient
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options

class InfoModule : RunnableModule {

    private val t = Option("t", "topics", true, "Comma-separated list of topics to get info for")
    private val f = Option("f", "filter-by-broker", true, "Gets info about topics that are on specific broker(s), " +
            "brokers filter is  comma-separated list of ids, addresses or racks in any combination. Works with one of --all-topics or --topics")
    private val a = Option("a", "all-topics", false, "Get info for all topics")
    private val b = Option("b", "brokers", false, "Get info about brokers")

    override fun module(): Module = Module.INFO

    override fun getOptions(): Options = Options().of(t, a, b)

    override fun getDescription(): String = "Gets info about cluster"

    override fun run(cli: CommandLine, kafkaAdminClient: KafkaAdminClient, dryRun: Boolean) {

        fun loadAssignments(topics: Set<String> = emptySet()): KafkaPartitionAssignment {
            return kafkaAdminClient.currentAssignment(topics).let { assignment ->
                if (cli.has(f)) {
                    val brokers = kafkaAdminClient.brokers()
                    val filterOutTokens = cli.get(f) { it.split(",").map { it.toLowerCase() }.toSet() } ?: emptySet()
                    assignment.copy(
                            partitions = assignment.partitions.filter { partition ->
                                val partitionBrokers = partition.replicas.map { brokers.getValue(it) }
                                partitionBrokers.any {
                                    it.address.toLowerCase() in filterOutTokens ||
                                            it.id.toString().toLowerCase() in filterOutTokens ||
                                            it.rack.toLowerCase() in filterOutTokens
                                }
                            }
                    )
                } else {
                    assignment
                }

            }
        }

        cli.ifHas(t) {
            val topics = cli.get(t) { it.split(",").toSet() } ?: emptySet()
            val assignment = loadAssignments(topics)
            printAssignment(assignment)
        }

        cli.ifHas(a) {
            val assignment = loadAssignments()
            printAssignment(assignment)
        }

        cli.ifHas(b) {
            val brokers = kafkaAdminClient.brokers()

            println("Brokers [${brokers.size}]: $brokers")
        }
    }


    private fun printAssignment(assignment: KafkaPartitionAssignment) {
        assignment.partitions
                .sortedBy { it.partition }
                .groupBy { it.topic }
                .forEach { (topic, partitions) ->
                    println("Topic: $topic")
                    partitions.forEach { p ->
                        println(String.format(
                                "\t%3s Replicas: [%s] ISR: [%s] Leader: %s",
                                p.partition,
                                p.replicas.joinToString(),
                                p.inSyncReplicas.joinToString(),
                                p.leader ?: "NONE"
                        ))
                    }
                }
    }
}