package kafka.suite

import kafka.suite.client.KafkaAdminClient
import kafka.suite.reassign.ReplaceAbsentNodeModule
import kafka.suite.reassign.ReplaceNodeModule
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.Options
import kotlin.reflect.KClass
import kotlin.reflect.full.primaryConstructor

enum class Module(val key: String, private val clazz: KClass<out RunnableModule>) {
    REPLACE_NODE("replace-node", ReplaceNodeModule::class),
    REPLACE_ABSENT_NODE("replace-absent-node", ReplaceAbsentNodeModule::class),
    ;

    val description: String
        get(): String = getInstance().getDescription()

    fun getInstance(): RunnableModule = this.clazz.primaryConstructor!!.call()

    companion object {
        fun byKey(key: String): Module? = values().first { it.key == key }
    }
}

interface RunnableModule {

    fun module(): Module

    fun getOptions(): Options

    fun getDescription(): String

    fun run(cli: CommandLine, kafkaAdminClient: KafkaAdminClient, dryRun: Boolean, waitToFinish: Boolean)
}

