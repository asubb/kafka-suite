package kafka.suite

import kafka.suite.client.KafkaAdminClient
import kafka.suite.replacenode.ReplaceNodeModule
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.Options
import kotlin.reflect.KClass
import kotlin.reflect.full.primaryConstructor

enum class Module(val key: String, val description: String, private val clazz: KClass<out RunnableModule>) {
    REPLACE_NODE("replace-node", "moves partitions from one node to another", ReplaceNodeModule::class);

    fun getInstance(): RunnableModule {
        return this.clazz.primaryConstructor!!.call()
    }

    companion object {
        fun byKey(key: String): Module? = values().first { it.key == key }
    }
}

interface RunnableModule {

    fun module(): Module

    fun getOptions(): Options

    fun run(cli: CommandLine, kafkaAdminClient: KafkaAdminClient, dryRun: Boolean, waitToFinish: Boolean)
}

