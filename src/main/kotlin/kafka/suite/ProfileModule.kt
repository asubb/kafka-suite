package kafka.suite

import kafka.suite.client.KafkaAdminClient
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options

class ProfileModule : RunnableModule {

    private val n = Option("n", "name", true, "Profile name")
    private val view = Option(null, "view", false, "View stored profiles or just selected one.")
    private val z = Option("z", "zookeeper", true, "Comma-separated list of ZK servers, i.e. `zoo1:2181,zoo2:2181/root`.")
    private val b = Option("b", "brokers", true, "Comma-separated list of brokers, i.e. `kafka1:9092,127.0.0.1:9092`.")
    private val kafkaBin = Option(null, "kafka-bin", true, "For older version clusters the path to kafka cli binaries is required.")
    private val zkClient = Option(null, "zk-client", false, "Use ZK client which performs some actions in ZK directly as Kafka client doesn't provide needed functionality.")
    private val plain = Option(null, "plain", false, "Use built-in to Kafka client. Preferred.")
    private val a = Option("a", "add-weight", true, "Comma-separated entities to add weight for, i.e.: topic1:1:2:3,topic2. It works just like a key. Requires to specify weight as different options. Please make sure weight metrics are more or less normalized. Works only if profile exists.")
    private val d = Option("d", "delete-weight", true, "Comma-separated entities to remove weight for, i.e.: topic1:1:2:3,topic2. It works just like a key.")
    private val s = Option("s", "size", true, "Weight Property. Size of the single partition.")
    private val c = Option("c", "cpu", true, "Weight Property. CPU credits used by partition.")
    private val m = Option("m", "memory", true, "Weight Property. Memory credits used by partition.")
    private val w = Option("w", "write-rate", true, "Weight Property. Write rate for partition.")
    private val r = Option("r", "read-rate", true, "Weight Property. Read rate for partition.")

    override fun module(): Module = Module.PROFILE

    override fun getOptions(): Options = Options().of(n, z, b, kafkaBin, zkClient, view, a, d, s, c, m, w, r)

    override fun getDescription(): String = "Create or update the profile, as well as activate it."

    override fun run(cli: CommandLine, kafkaAdminClient: KafkaAdminClient, dryRun: Boolean) {
        createProfile(cli)
    }

    fun createProfile(cli: CommandLine) {
        if (cli.has(view) && !cli.has(n)) {
            ClusterProfile.readAll().forEach { println(it) }
            return
        }
        val name = cli.getRequired(n) { it }

        val profile = ClusterProfile.read(name)

        if (profile == null) {
            println("Profile $name doesn't exist. Creating...")
            val zooKeeper = cli.getRequired(z) { it }
            val brokerList = cli.getRequired(b) { it }
            val kafkaBin = cli.get(kafkaBin) { it } ?: ""
            val zkClient = cli.has(zkClient)
            val plain = cli.has(plain)

            val p = ClusterProfile(
                    name,
                    true,
                    zooKeeper,
                    brokerList,
                    kafkaBin = if (!zkClient && !plain) kafkaBin else "",
                    zkClient = kafkaBin.isEmpty() && !plain && zkClient
            )
            p.save()
            println("Profile created: $p")
        } else {
            if (cli.has(view)) {
                println("Profile $profile")
            } else {
                var p = profile.copy(active = true)
                cli.ifHas(z) { p = cli.getRequired(z) { p.copy(zookeeper = it.first().toString()) } }
                cli.ifHas(b) { p = cli.getRequired(b) { p.copy(brokers = it.first().toString()) } }
                cli.ifHas(kafkaBin) { p = cli.getRequired(kafkaBin) { p.copy(kafkaBin = it.first().toString(), zkClient = false) } }
                cli.ifHas(zkClient) { p = p.copy(kafkaBin = "", zkClient = true) }
                cli.ifHas(a) {
                    val key = cli.getRequired(a) { it }
                    var current = p.weights?.get(key) ?: PartitionWeight.empty
                    cli.ifHas(s) { current = current.copy(size = cli.getRequired(s) { it.toLong() }) }
                    cli.ifHas(c) { current = current.copy(cpuCredits = cli.getRequired(c) { it.toInt() }) }
                    cli.ifHas(m) { current = current.copy(memoryCredits = cli.getRequired(m) { it.toInt() }) }
                    cli.ifHas(w) { current = current.copy(writeRate = cli.getRequired(w) { it.toLong() }) }
                    cli.ifHas(r) { current = current.copy(readRate = cli.getRequired(r) { it.toLong() }) }

                    p = p.copy(weights = (p.weights?.filter { it.key != key } ?: emptyMap()).plus(key to current))
                }
                cli.ifHas(d) {
                    val key = cli.getRequired(d) { it }
                    p = p.copy(weights = p.weights?.filter { it.key != key })
                }
                p.save()
                println("Profile updated: $p")
            }
        }
    }
}