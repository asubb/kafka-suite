package kafka.suite

import kafka.suite.client.KafkaAdminClient
import kafka.suite.reassign.ProfileBasedWeightFn
import kafka.suite.reassign.WeightFns
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import kotlin.math.abs

class InfoModule : RunnableModule {

    private val t = Option("t", "topics", true, "Comma-separated list of topics to get info for")
    private val f = Option("f", "filter-by-broker", true, "Gets info about topics that are on specific broker(s), " +
            "brokers filter is  comma-separated list of ids, addresses or racks in any combination. Works with one of --all-topics or --topics")
    private val a = Option("a", "all-topics", false, "Get info for all topics")
    private val b = Option("b", "brokers", false, "Get info about brokers")
    private val e = Option("e", "extended", false, "Prints extended partition information that includes some metrics " +
            "based on your profile, i.e. the weight of the partitions. Works with one of --all-topics or --topics")
    private val w = Option("w", "weightFn", true, "The name of the weight function: " + WeightFns.values().joinToString { it.id } + ".")

    override fun module(): Module = Module.INFO

    override fun getOptions(): Options = Options().of(t, a, b, f, e, w)

    override fun getDescription(): String = "Gets info about cluster"

    override fun run(cli: CommandLine, kafkaAdminClient: KafkaAdminClient, dryRun: Boolean) {

        fun loadAssignments(topics: Set<String> = emptySet()): KafkaPartitionAssignment {
            return kafkaAdminClient.currentAssignment(topics).let { assignment ->
                if (cli.has(f)) {
                    val brokers = kafkaAdminClient.brokers()
                    val filterOutTokens = cli.get(f) { it.split(",").map { it.toLowerCase() }.toSet() } ?: emptySet()
                    assignment.copy(
                            partitions = assignment.partitions.filter { partition ->
                                val partitionBrokers = partition.replicas.map {
                                    brokers[it] ?: KafkaBroker(-it, "<UNDEFINED>", "<NONE>")
                                }
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
            printAssignment(assignment, cli.has(e))
        }

        cli.ifHas(a) {
            val assignment = loadAssignments()
            printAssignment(assignment, cli.has(e))
        }

        cli.ifHas(b) {
            val brokers = kafkaAdminClient.brokers()

            val weightFn = cli.get(w) { w -> WeightFns.values().first { it.id.toLowerCase() == w.toLowerCase() }.creatorFn() }
            val assignments = loadAssignments()

            val b = brokers
                    .map { broker ->
                        broker.value to (weightFn?.let { weightFn ->
                            assignments.partitions.filter { broker.key in it.replicas }
                                    .map { weightFn.weight(it) }
                                    .sum()
                        } ?: 0L)
                    }
                    .sortedBy { it.second }.joinToString("\n") { broker ->
                        String.format("[%5d] %s (%s)",
                                broker.first.id,
                                broker.first.address,
                                broker.first.rack
                        ) + (weightFn
                                ?.let { String.format(" Weight: %s", partitionWeightAsString(broker.second)) }
                                ?: "")

                    }
            println("Brokers [${brokers.size}]: \n$b")
        }
    }


    private fun printAssignment(assignment: KafkaPartitionAssignment, extended: Boolean) {
        val weightFn = ProfileBasedWeightFn()
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
                        ) +
                                if (extended) {
                                    String.format(" Weight: %s", partitionWeightAsString(weightFn.weight(p)))
                                } else {
                                    ""
                                }
                        )
                    }
                }
    }

    private fun partitionWeightAsString(w: Long): String {
        val v = when {
            abs(w) < 1_000 -> Pair((w.toDouble()), "")
            abs(w) >= 1e3 -> Pair((w / 1e3), "K")
            abs(w) >= 1e6 -> Pair((w / 1e6), "M")
            abs(w) >= 1e9 -> Pair((w / 1e9), "G")
            abs(w) >= 1e12 -> Pair((w / 1e12), "T")
            abs(w) >= 1e15 -> Pair((w / 1e15), "P")
            else -> Pair(w.toDouble(), "O_O don't you need to check your weights?")
        }
        return String.format("%.2f%s", v.first, v.second)
    }
}