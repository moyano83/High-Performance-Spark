package com.spark.chapter3

import com.spark.chapter3.example.model.{PandaPlace, RawPanda}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types._

object Example1 {

  def main(args:Array[String]):Unit = {
    val session = SparkSession.builder().master("local[1]").enableHiveSupport().getOrCreate()
    import session.implicits._

    val damao1 = RawPanda(1L, "M1B", "Giant", true, Array(0.1, 0.2))
    val damao2 = RawPanda(2L, "M2B", "Midget", false, Array(0.3, 0.2))
    val place = PandaPlace("Madrid", Array(damao1, damao2))

    val df = session.createDataFrame(Seq(place))
    df.printSchema()
    df.filter(df("happy") =!= true)

    val pandaInfo = df.explode(df("pandas")){
      case Row(pandas:Seq[Row]) => pandas.map {
        case Row(id: Long, zip: String, pt: String, happy: Boolean, attributes: Seq[Double]) =>
          RawPanda(id, zip, pt, happy, attributes.toArray)
      }
    }

    pandaInfo.select(pandaInfo("attributes")(0) / pandaInfo("attributes")(1)).as("squishyness")
    Window.orderBy(pandaInfo("id")).partitionBy("pt").rangeBetween(-10,10)

    df.select("pandas").
  }

  def encodePandaType(pandaInfo:DataFrame): Unit ={
    pandaInfo.select(pandaInfo("id"), (when(pandaInfo("pt") === "Giant", 0)
      .when(pandaInfo("pt") === "red", 1)
      .otherwise(2)).as("encodedType"))
  }

  def minMeanSizePerZip(df:DataFrame):DataFrame = {
    df.groupBy("pandas").agg(min(df("id")), mean(df("attributes")))
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

  def maxPandaSizePerZip(ds:Dataset[RawPanda]): Dataset[(String, Double)] = {
    ds.groupByKey(_.zip).agg(max("size").as[Double])
  }
}

