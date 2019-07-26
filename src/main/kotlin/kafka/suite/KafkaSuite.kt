package kafka.suite

import kafka.suite.client.CliKafkaAdminClient
import kafka.suite.client.ScalaKafkaAdminClient
import org.apache.commons.cli.*
import java.io.PrintWriter
import java.lang.System.exit

private val b = Option("b", "bootstrap-server", true, "Kafka bootstrap servers list.").required()
private val z = Option("z", "zookeeper", true, "Zookeeper connection string kafka is connected to.").required()
private val d = Option("d", "dry-run", false, "Do not perform actually, just print out an intent. By default it runs really.")
private val w = Option("w", "no-wait", false, "Do not wait for job to finish. By default it waits")
private val h = Option("h", "help", false, "Shows this help, or if module specified shows module help")
private val k = Option("k", "kafka", true, "For older version clusters the path to kafka cli binaries is required.")

fun main(args: Array<String>) {
    val options = Options().of(b, z, d, w, h, k)

    if (args.isEmpty()) {
        printGeneralHelp(options)
    } else {
        val module = Module.byKey(args[0])

        val runnableModule = module?.getInstance()
        runnableModule?.getOptions()?.options?.forEach { options.addOption(it) }

        val cli = try {
            DefaultParser().parse(options, args.copyOfRange(1, args.size))
        } catch (e: MissingOptionException) {
            null
        } catch (e: MissingArgumentException) {
            println(e.message)
            null
        }
        val help = cli?.get(h, false) { true } ?: true

        when {
            runnableModule == null || cli == null -> printGeneralHelp(options)
            help -> printModuleHelp(runnableModule)
            else -> runModule(cli, runnableModule)
        }
    }
}

private fun runModule(cli: CommandLine, runnableModule: RunnableModule) {
    try {
        val bootstrapServer = cli.get(b) { it.first().toString() }
        val zkConnectionString = cli.get(z) { it.first().toString() }
        val dryRun = cli.get(d, false) { true }
        val waitToFinish = cli.get(w, true) { false }
        val kafkaCliPath = cli.get(k, "") { it.first().toString() }

        val c = if (kafkaCliPath.isEmpty())
            ScalaKafkaAdminClient(bootstrapServer, zkConnectionString)
        else
            CliKafkaAdminClient(bootstrapServer, zkConnectionString, kafkaCliPath)

        runnableModule.run(cli, c, dryRun, waitToFinish)
    } catch (e: IllegalArgumentException) {
        println("ERROR: ${e.message}")
        e.printStackTrace(System.err)
        exit(2)
    }
}

private fun printModuleHelp(module: RunnableModule) {
    val formatter = HelpFormatter()
    val writer = PrintWriter(System.out)
    val options = module.getOptions()
    formatter.printUsage(writer, 80, "ksuite ${module.module().key}", options)
    formatter.printOptions(writer, 80, options, 0, 0)
    writer.flush()
    exit(1)
}

private fun printGeneralHelp(options: Options?) {
    val formatter = HelpFormatter()
    val writer = PrintWriter(System.out)
    formatter.printUsage(writer, 80, "ksuite <module>", options)
    formatter.printOptions(writer, 80, options, 0, 0)
    writer.flush()
    println()
    println("Available modules:")
    Module.values().forEach {
        println("${it.key} - ${it.description}")
    }
    exit(1)
}
