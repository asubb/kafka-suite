package kafka.suite

import kafka.suite.client.CliKafkaAdminClient
import kafka.suite.client.ScalaKafkaAdminClient
import org.apache.commons.cli.*
import java.io.PrintWriter
import java.lang.System.exit

private val d = Option("d", "dry-run", false, "Do not perform actually, just print out an intent. By default it runs really.")
private val w = Option("w", "no-wait", false, "Do not wait for job to finish. By default it waits")
private val h = Option("h", "help", false, "Shows general help, or if module specified shows module help")

val options = Options().of(d, w, h)

fun main(args: Array<String>) {

    if (args.isEmpty()) {
        printGeneralHelp()
    } else {
        val module = Module.byKey(args[0])

        val runnableModule = module?.getInstance()
        runnableModule?.getOptions()?.options?.forEach { options.addOption(it) }

        if (runnableModule != null && (args.size > 1 && args[1].trim() == "-h" || args.size == 1)) {
            printModuleHelp(runnableModule)
        } else {
            val cli = try {
                DefaultParser().parse(options, args.copyOfRange(1, args.size))
            } catch (e: MissingOptionException) {
                println(e.message)
                null
            } catch (e: MissingArgumentException) {
                println(e.message)
                null
            }
            when {
                runnableModule == null && cli == null -> printGeneralHelp()
                runnableModule != null && cli == null -> printModuleHelp(runnableModule)
                runnableModule == null && cli != null -> printGeneralHelp()
                runnableModule != null && cli != null -> runModule(cli, runnableModule)
            }
        }
    }
}

private fun runModule(cli: CommandLine, runnableModule: RunnableModule) {
    try {
        if (runnableModule is ProfileModule) {
            ProfileModule().createProfile(cli)
        } else {
            val profile = ClusterProfile.loadActive()
            println("Using profile: $profile")
            val bootstrapServer = profile.brokers
            val zkConnectionString = profile.zookeeper
            val kafkaCliPath = profile.kafkaBin
            val dryRun = cli.get(d, false) { true }
            val waitToFinish = cli.get(w, true) { false }

            val c = if (kafkaCliPath.isEmpty())
                ScalaKafkaAdminClient(bootstrapServer, zkConnectionString)
            else
                CliKafkaAdminClient(bootstrapServer, zkConnectionString, kafkaCliPath)

            runnableModule.run(cli, c, dryRun, waitToFinish)
        }

    } catch (e: IllegalArgumentException) {
        println("ERROR: ${e.message}")
        e.printStackTrace(System.err)
        exit(2)
    }
}

private fun printModuleHelp(module: RunnableModule) {
    val formatter = HelpFormatter()
    val writer = PrintWriter(System.out)
    formatter.printUsage(writer, 80, "ksuite ${module.module().key}", options)
    writer.println()
    writer.println(module.getDescription())
    writer.println()
    formatter.printOptions(writer, 80, options, 0, 0)
    writer.flush()
    exit(1)
}

private fun printGeneralHelp() {
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
