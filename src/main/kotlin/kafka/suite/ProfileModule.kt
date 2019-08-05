package kafka.suite

import kafka.suite.client.KafkaAdminClient
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options

class ProfileModule : RunnableModule {

    private val n = Option("n", "name", true, "Profile name").required()
    private val z = Option("z", "zookeeper", true, "Comma-separated list of ZK servers, i.e. `zoo1:2181,zoo2:2181/root`.")
    private val b = Option("b", "brokers", true, "Comma-separated list of brokers, i.e. `kafka1:9092,127.0.0.1:9092`.")
    private val r = Option("r", "rack", true, "Comma-separated list of broker-rack correspondence. " +
            "If it can't be fetched by client. Example: `1040:AZ1,1039:AZ2`, if there is no racks, just specify nothing")
    private val kafkaBin = Option(null, "kafka-bin", true, "For older version clusters the path to kafka cli binaries is required.")
    private val zkClient = Option(null, "zk-client", false, "Use ZK client which performs some actions in ZK directly as Kafka client doesn't provide needed functionality.")
    private val plain = Option(null, "plain", false, "Use built-in to Kafka client. Preferred.")

    override fun module(): Module = Module.PROFILE

    override fun getOptions(): Options = Options().of(n, z, b, r, kafkaBin, zkClient)

    override fun getDescription(): String = "Create or update the profile, as well as activate it."

    override fun run(cli: CommandLine, kafkaAdminClient: KafkaAdminClient, dryRun: Boolean) {
        createProfile(cli)
    }

    fun createProfile(cli: CommandLine) {
        val name = cli.getRequired(n) { it }

        val profile = ClusterProfile.read(name)

        if (profile == null) {
            val zooKeeper = cli.getRequired(z) { it }
            val brokerList = cli.getRequired(b) { it }
            val userDefinedBrokerRack = cli.get(r) { parseBrokerRack(it) } ?: emptyMap()
            val kafkaBin = cli.get(kafkaBin) { it } ?: ""
            val zkClient = cli.has(zkClient)
            val plain = cli.has(plain)

            ClusterProfile(
                    name,
                    true,
                    zooKeeper,
                    brokerList,
                    userDefinedBrokerRack,
                    kafkaBin = if (!zkClient && !plain) kafkaBin else "",
                    zkClient = kafkaBin.isEmpty() && !plain && zkClient
            ).save()
        } else {
            var p = profile.copy(active = true)
            cli.ifHas(z) { p = cli.getRequired(z) { p.copy(zookeeper = it.first().toString()) } }
            cli.ifHas(b) { p = cli.getRequired(b) { p.copy(brokers = it.first().toString()) } }
            cli.ifHas(r) { p = cli.getRequired(r) { p.copy(racks = parseBrokerRack(it)) } }
            cli.ifHas(kafkaBin) { p = cli.getRequired(kafkaBin) { p.copy(kafkaBin = it.first().toString(), zkClient = false) } }
            cli.ifHas(zkClient) { p = p.copy(kafkaBin = "", zkClient = true) }
            p.save()
        }
    }

    private fun parseBrokerRack(arg: String): Map<Int, String> {
        return arg.split(",").asSequence()
                .map {
                    val (brokerId, rack) = it.split(":", limit = 2)
                    val key = brokerId.toInt()
                    key to rack
                }
                .toMap()
    }

}