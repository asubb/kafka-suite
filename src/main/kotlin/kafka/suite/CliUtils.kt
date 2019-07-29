package kafka.suite

import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options

fun <T> CommandLine.get(option: Option, default: T? = null, converter: (List<*>) -> T): T {
    return this.options
            .firstOrNull { it == option }
            ?.let {
                val valuesList = it.valuesList
                if (option.hasArgs() && valuesList.isEmpty()) {
                    default ?: throw IllegalArgumentException("${option.argName} should have value")
                } else {
                    converter(valuesList)
                }
            }
            ?: default
            ?: throw IllegalArgumentException("`${option.longOpt ?: option.opt}` is not specified but required")
}

fun Options.of(vararg o: Option): Options {
    o.forEach { this.addOption(it) }
    return this
}

fun Option.required(): Option {
    this.isRequired = true
    return this
}

fun CommandLine.ifHas(o: Option, body: (CommandLine) -> Unit) {
    if (this.hasOption(o.opt))
        body(this)
}

fun CommandLine.has(o: Option): Boolean {
    return this.hasOption(o.opt)
}
