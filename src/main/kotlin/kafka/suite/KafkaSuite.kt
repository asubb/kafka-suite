package kafka.suite

import kafka.suite.module.Module
import kafka.suite.module.RunnableModule
import org.apache.commons.cli.*
import java.io.PrintWriter
import java.lang.System.exit

fun main(args: Array<String>) {


    val b = Option("b", "bootstrap-server", true, "Kafka bootstrap servers list.").required()
    val z = Option("z", "zookeeper", true, "Zookeeper connection string kafka is connected to.").required()
    val d = Option("d", "dry-run", false, "Do not perform actually, just print out an intent. By default it runs really.")
    val w = Option("w", "no-wait", false, "Do not wait for job to finish. By default it waits")
    val h = Option("h", "help", false, "Shows this help, or if module specified shows module help")

    val options = Options().of(b, z, d, w, h)


    if (args.isEmpty()) {
        printGeneralHelp(options)
    } else {
        val module = Module.byKey(args[0])

        val runnableModule = module?.getInstance()
        runnableModule?.getOptions()?.options?.forEach { options.addOption(it) }

        val cli = DefaultParser().parse(options, args.copyOfRange(1, args.size))
        val help = cli.get(h, false) { true }

        when {
            runnableModule == null -> printGeneralHelp(options)
            help -> printModuleHelp(runnableModule)
            else -> runModule(cli, b, z, d, w, runnableModule)
        }

    }
}

private fun runModule(cli: CommandLine, b: Option, z: Option, d: Option, w: Option, runnableModule: RunnableModule) {
    try {
        val bootstrapServer = cli.get(b) { it.first().toString() }
        val zkConnectionString = cli.get(z) { it.first().toString() }
        val dryRun = cli.get(d, false) { true }
        val waitToFinish = cli.get(w, true) { false }

        val c = KafkaAdminClient(bootstrapServer, zkConnectionString)
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
