package kafka.suite

import assertk.Assert
import assertk.assertions.isNotIn
import assertk.assertions.support.expected
import assertk.assertions.support.show

/**
 * Asserts the value is in the expected values, using `in`.
 * @see [isNotIn]
 */
fun <T> Assert<Set<T>>.intersects(values: Set<T>) = given { actual ->
    if (actual.all { it in values }) return
    expected(":${show(values)} to contain:${show(actual)}")
}
