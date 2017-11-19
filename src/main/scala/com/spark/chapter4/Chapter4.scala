package com.spark.chapter4

import com.spark.model.{PandaPlace, RawPanda}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Chapter4 {

  val damao1 = RawPanda(1L, "M1B", "Giant", true, Array(0.1, 0.2))
  val damao2 = RawPanda(2L, "M2B", "Midget", false, Array(0.3, 0.2))
  val damao3 = RawPanda(2L, "C3A", "Red", false, Array(0.2, 0.1))
  val place1 = PandaPlace("Madrid", Array(damao1, damao2))
  val place2 = PandaPlace("Barcelona", Array(damao2, damao3))


  def main(args:Array[String]):Unit = {
    val session = SparkSession.builder().enableHiveSupport().appName("Chapter4").master("local[1]").getOrCreate()
    import session.implicits._
    val rdd1 = session.sparkContext.parallelize(Seq(place1))
    val rdd2 = session.sparkContext.parallelize(Seq(place2))

    val df1 = session.createDataFrame(Seq(place1))
    val df2 = session.createDataFrame(Seq(place2))

    val joinRdd = joinRDD(rdd1, rdd2);
    val joinedDF = joinDF(df1, df2)
    joinRdd.collect()
    joinedDF.collect()
  }

  def joinRDD(rdd1:RDD[PandaPlace], rdd2:RDD[PandaPlace]): RDD[(String, (Iterable[PandaPlace], Iterable[PandaPlace]))] = {
    rdd1.groupBy(_.name).join(rdd2.groupBy((_.name)))
  }

  def joinDF(df1:DataFrame, df2:DataFrame): DataFrame = {
    df1.join(broadcast(df2), df1("name") === df2("name"), "inner")
  }
}
