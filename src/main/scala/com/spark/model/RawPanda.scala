package com.spark.model

case class RawPanda(id:Long, zip:String, pt:String, happy:Boolean, attributes:Array[Double]){
  override def toString: String = id.toString
}
