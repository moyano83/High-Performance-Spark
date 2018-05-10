package com.spark.chapter5

import com.spark.model.RawPanda
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Chapter5 extends App{

  def computeTotalFuzzyness(sc:SparkContext, rdd:RDD[RawPanda]):(RDD[(String, Long)], Double) = {
    val acc = sc.doubleAccumulator("fuzzyness")
    val transformed = rdd.map{p => acc.add(
      p.attributes(0))
      (p.zip, p.id)
    }


    val zip = sc.longAccumulator("zip")
    rdd.foreach(p=> zip.add(p.id))

    transformed.count()
    (transformed, acc.value)
  }
}
