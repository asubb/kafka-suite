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
    private val k = Option("k", "kafka", true, "For older version clusters the path to kafka cli binaries is required.")

    override fun module(): Module = Module.PROFILE

    override fun getOptions(): Options = Options().of(n, z, b, r)

    override fun getDescription(): String = "Create or update the profile, as well as activate it."

    override fun run(cli: CommandLine, kafkaAdminClient: KafkaAdminClient, dryRun: Boolean, waitToFinish: Boolean) {
        createProfile(cli)
    }

    fun createProfile(cli: CommandLine) {
        val name = cli.get(n) { it.first().toString() }

        val profile = ClusterProfile.read(name)

        if (profile == null) {
            val zooKeeper = cli.get(z) { it.first().toString() }
            val brokerList = cli.get(b) { it.first().toString() }
            val userDefinedBrokerRack = cli.get(r, emptyMap()) { parseBrokerRack(it) }
            val kafkaBin = cli.get(k, "") { it.first().toString() }

            ClusterProfile(
                    name,
                    true,
                    zooKeeper,
                    brokerList,
                    userDefinedBrokerRack,
                    kafkaBin
            ).save()
        } else {
            var p = profile.copy(active = true)
            cli.ifHas(z) { p = cli.get(z) { p.copy(zookeeper = it.first().toString()) } }
            cli.ifHas(b) { p = cli.get(b) { p.copy(brokers = it.first().toString()) } }
            cli.ifHas(r) { p = cli.get(r) { p.copy(racks = parseBrokerRack(it)) } }
            cli.ifHas(k) { p = cli.get(k) { p.copy(kafkaBin = it.first().toString()) } }
            p.save()
        }
    }

    private fun parseBrokerRack(it: List<*>): Map<Int, String> {
        return it.toString().split(",").asSequence()
                .map {
                    val (brokerId, rack) = it.split(":", limit = 2)
                    val key = brokerId.toInt()
                    key to rack
                }
                .toMap()
    }

}