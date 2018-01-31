package com.spark.chapter6

import com.spark.model.RawPanda
import org.apache.spark.{HashPartitioner, Partitioner}

/**
  * Created by jm186111 on 31/01/2018.
  */
class PrimaryKeyPartitioner[K, S](partitions:Int) extends Partitioner{

  val delegatePartitioner = new HashPartitioner(partitions)

  override def numPartitions = delegatePartitioner.numPartitions

  override def getPartition(key: Any) = {
    delegatePartitioner.getPartition(key.asInstanceOf[(K, S)]._1)
  }


  implicit def orderByLocationAndName[A <: RawPanda]: Ordering[A] = { Ordering.by(pandaKey => (pandaKey.id, pandaKey
    .zip, pandaKey.pt))
  }
}
