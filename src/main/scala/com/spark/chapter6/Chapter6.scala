package com.spark.chapter6

import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object Chapter6 {

  def groupByKeyAndSortBySecondaryKey[K : Ordering : ClassTag, S : Ordering : ClassTag,
  V : ClassTag]
  (pairRDD : RDD[((K, S), V)], partitions : Int):
  RDD[(K, List[(S, V)])] = {
    //Create an instance of our custom partitioner
    val colValuePartitioner = new PrimaryKeyPartitioner[Double, Int](partitions)
    //define an implicit ordering, to order by the second key the ordering will //be used even though not explicitly called
    implicit val ordering: Ordering[(K, S)] = Ordering.Tuple2
    //use repartitionAndSortWithinPartitions
    val sortedWithinParts = pairRDD.repartitionAndSortWithinPartitions(colValuePartitioner)
    sortedWithinParts.mapPartitions( iter => groupSorted[K, S, V](iter) )

  }
  def groupSorted[K,S,V](it: Iterator[((K, S), V)]): Iterator[(K, List[(S, V)])] = {
    val res = List[(K, ArrayBuffer[(S, V)])]()
    it.foldLeft(res)((list, next) => list match {
    case Nil =>
      val ((firstKey, secondKey), value) = next
      List((firstKey, ArrayBuffer((secondKey, value))))
    case head :: rest =>
      val (curKey, valueBuf) = head
      val ((firstKey, secondKey), value) = next
      if (!firstKey.equals(curKey) ) {
      (firstKey, ArrayBuffer((secondKey, value))) :: list } else {
      valueBuf.append((secondKey, value))
      list
    }
  }).map { case (key, buf) => (key, buf.toList) }.iterator }
}
