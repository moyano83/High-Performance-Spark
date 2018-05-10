package com.spark.chapter3

import com.spark.model.{PandaPlace, RawPanda}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types._

object Chapter3 {

  def main(args:Array[String]):Unit = {
    val session = SparkSession.builder().master("local[1]").enableHiveSupport().getOrCreate()
    import session.implicits._

    val damao1 = RawPanda(1L, "M1B", "Giant", true, Array(0.1, 0.2))
    val damao2 = RawPanda(2L, "M2B", "Midget", false, Array(0.3, 0.2))
    val place = PandaPlace("Madrid", Array(damao1, damao2))

    val df = session.createDataFrame(Seq(place))
    df.printSchema()

    val pandas = df.select(df("pandas").as[Array[RawPanda]]).flatMap(p=>p).filter(p => p.happy != true)
    val pandas2 = session.createDataFrame(Seq(damao1, damao2)).filter(!$"happy").as[RawPanda] //Equivalent to the above

    val pandaInfo = df.explode(df("pandas")){
      case Row(pandas:Seq[Row]) => pandas.map {
        case Row(id: Long, zip: String, pt: String, happy: Boolean, attributes: Seq[Double]) =>
          RawPanda(id, zip, pt, happy, attributes.toArray)
      }
    }

    val squishynessPandas = pandaInfo.select(pandaInfo("attributes")(0) / pandaInfo("attributes")(1)).as("squishyness")
    val windowSpec = Window.orderBy(pandaInfo("id")).partitionBy("pt").rangeBetween(-10,10)

    val sizeDeviation = pandaInfo("size") - avg(pandaInfo("size")).over(windowSpec)
    val maxVal = pandaInfo.groupBy($"id").agg(max(pandaInfo("attributes")(0)))
  }

  def encodePandaType(pandaInfo:DataFrame): Unit ={
    pandaInfo.select(pandaInfo("id"), (when(pandaInfo("pt") === "Giant", 0)
      .when(pandaInfo("pt") === "red", 1)
      .otherwise(2)).as("encodedType"))
  }

  def minMeanSizePerZip(df:DataFrame, sparkSession: SparkSession):DataFrame = {
    import sparkSession.implicits._
    df.groupBy("pandas").agg(min(df("id")), mean(df("attributes")))
  }

  def maxPandaSizePerZip(ds:Dataset[RawPanda], sparkSession: SparkSession): Dataset[(String, Double)] = {
    import sparkSession.implicits._
    ds.groupByKey(_.zip).agg(max("size").as[Double])
  }


  def createSchema()={
    StructType(Array(
      StructField("name", StringType, false),
      StructField("pandas",StructType(Array(
        StructField("id", LongType, false),
        StructField("zip", StringType, false),
        StructField("pt", StringType, false),
        StructField("happy", BooleanType, false),
        StructField("attributes", ArrayType(DoubleType, false), false))), false)))
  }

}
