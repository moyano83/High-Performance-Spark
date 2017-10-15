package com.spark.chapter3.example.model

case class RawPanda(id:Long, zip:String, pt:String, happy:Boolean, attributes:Array[Double])

case class PandaPlace(name:String, pandas:Array[RawPanda])
