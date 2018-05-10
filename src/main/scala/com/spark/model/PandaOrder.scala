package com.spark.model

class PandaOrder extends Ordering[RawPanda]{
  override def compare(x: RawPanda, y: RawPanda): Int = if(x.id >= y.id) 1 else -1
}
