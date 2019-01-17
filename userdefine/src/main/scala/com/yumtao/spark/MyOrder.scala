package com.yumtao.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yumtao on 2019/1/17.
  */
object MyOrder {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MyOrder").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val scrGrils = sc.parallelize(Array(("sijiali", 95, 30), ("menglu", 89, 50), ("beiluqi", 96, 45), ("sufei", 90, 40), ("qingqing", 96, 20)))
    val girls = scrGrils.map(tmp => new GirlScore(tmp._1, tmp._2, tmp._3) with Serializable)
    val orderedGrils = girls.sortBy(GirlSort)
    println(orderedGrils.collect.toBuffer)

    sc.stop()
  }

}

case class GirlScore(val name: String, val faceScore: Int, val age: Int)

// 对faceValue和age进行分别比较，先按age，再按faceValue进行排序
case class GirlSort(val girl: GirlScore) extends Ordered[GirlSort] with Serializable {
  override def compare(that: GirlSort): Int = {
    val faceScore = that.girl.faceScore - this.girl.faceScore
    val ageScore = this.girl.age - that.girl.age

    // 1. 颜值高, 2.年龄小, 3.随便按名字排吧
    if (faceScore != 0) faceScore else if (ageScore != 0) ageScore
    else this.girl.name.compareTo(that.girl.name)
  }
}