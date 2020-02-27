package kafka.suite

import kafka.suite.client.CliKafkaAdminClient
import kafka.suite.client.ScalaKafkaAdminClient
import org.apache.commons.cli.*
import java.io.PrintWriter
import java.lang.System.exit
import org.slf4j.LoggerFactory
import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import kafka.suite.client.ZkKafkaAdminClient
import java.util.jar.JarFile
import java.util.jar.Manifest

private val d = Option("d", "dry-run", false, "Do not perform actually, just print out an intent. By default it runs really.")
private val h = Option("h", "help", false, "Shows general help, or if module specified shows module help")
private val debug = Option(null, "debug", false, "Runs logger in debug mode")

val options = Options().of(d, h, debug)

fun main(args: Array<String>) {

    if (args.isEmpty()) {
        printGeneralHelp()
    } else {
        val module = Module.byKey(args[0])

        val runnableModule = module?.getInstance()
        runnableModule?.getOptions()?.options?.forEach { options.addOption(it) }

        if (runnableModule != null && (
                        args.size > 1 && (args[1].trim() == "-h" || args[1].trim() == "--help")
                                || args.size == 1 && runnableModule.getOptions().options.any { it.isRequired }
                        )
        ) {
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
            cli?.ifHas(debug) {
                val root = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME) as Logger
                root.level = Level.DEBUG
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
            val dryRun = cli.has(d)

            val c = when {
                kafkaCliPath.isNotEmpty() -> CliKafkaAdminClient(bootstrapServer, zkConnectionString, kafkaCliPath)
                profile.zkClient ?: false -> ZkKafkaAdminClient(bootstrapServer, zkConnectionString)
                else -> ScalaKafkaAdminClient(bootstrapServer, zkConnectionString)
            }

            runnableModule.run(cli, c, dryRun)
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
    formatter.printWrapped(writer, 80, "KSuite v.${readVersion()} ")
    formatter.printUsage(writer, 80, "${readName()} <module>", options)
    formatter.printOptions(writer, 80, options, 0, 0)
    writer.flush()
    println()
    println("Available modules:")
    Module.values().forEach {
        println("${it.key} - ${it.description}")
    }
    exit(1)
}

private fun readVersion(): String {
    return Thread.currentThread().contextClassLoader.getResources(JarFile.MANIFEST_NAME)
            .asSequence()
            .mapNotNull {
                it.openStream().use { stream ->
                    Manifest(stream).mainAttributes.getValue("KSuite-Version")
                }
            }
            .firstOrNull()
            ?: "<NOT VERSIONED>"
}

private fun readName(): String {
    return Thread.currentThread().contextClassLoader.getResources(JarFile.MANIFEST_NAME)
            .asSequence()
            .mapNotNull {
                it.openStream().use { stream ->
                    Manifest(stream).mainAttributes.getValue("KSuite-Name")
                }
            }
            .firstOrNull()
            ?: "<NOT NAMED>"
}