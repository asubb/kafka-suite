package kafka.suite.util

import scala.Tuple2
import scala.collection.JavaConverters
import java.util.HashSet

fun <K, V> Map<K, V>.asScala(): scala.collection.Map<K, V> {
    val b = scala.collection.`Map$`.`MODULE$`.newBuilder<K, V>()
    this.forEach {
        b.`$plus$eq`(Tuple2(it.key, it.value))
    }
    @Suppress("UNCHECKED_CAST")
    return b.result() as scala.collection.Map<K, V>
}

fun <K, V> fromScala(map: scala.collection.Map<K, V>): Map<K, V> = JavaConverters.mapAsJavaMap(map)

fun <V> fromScala(seq: scala.collection.Seq<V>): List<V> = JavaConverters.seqAsJavaList(seq)

fun <V> Set<V>.asScala(): scala.collection.immutable.Set<V> = scala.collection.JavaConverters.asScalaSetConverter(HashSet<V>(this)).asScala().toSet()

fun <V> Set<V>.asScalaSeq(): scala.collection.Seq<V> = this.asScala().toSeq()

fun <V> V?.toScalaOption(): scala.Option<V> = scala.Option.apply(this)

