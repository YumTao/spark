package com.yumtao.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yumtao on 2019/1/17.
  */

/**
  * 方式一: 自定义类继承Ordered/Ordering,重写排序方法
  */
//case class Girl(name: String, faceScore: Int, age: Int) extends Ordered[Girl] with Serializable {
//  // 对faceValue和age进行分别比较，先按age，再按faceValue进行排序
//  override def compare(that: Girl): Int = {
//    val faceScore = this.faceScore - that.faceScore
//    val ageScore = -(this.age - that.age)
//
//    // 1. 颜值高, 2.年龄小, 3.随便按名字排吧
//    if (faceScore != 0) faceScore else if (ageScore != 0) ageScore
//    else this.name.compareTo(that.name)
//  }
//}

/**
  * 方式二:使用隐式转换
  */
case class Girl(name: String, faceScore: Int, age: Int) extends Serializable

object MyPreDef {
  implicit val girlToOrdered = new Ordering[Girl] {
    override def compare(x: Girl, y: Girl): Int = {
      val faceScore = x.faceScore - y.faceScore
      val ageScore = -(x.age - y.age)

      // 1. 颜值高, 2.年龄小, 3.随便按名字排吧
      if (faceScore != 0) faceScore else if (ageScore != 0) ageScore
      else x.name.compareTo(y.name)
    }
  }
}

object MyOrder {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MyOrder").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val scrGirls = sc.parallelize(Array(("sijiali", 95, 30), ("menglu", 89, 50), ("beiluqi", 96, 45), ("sufei", 90, 40), ("qingqing", 96, 20)))
    val girls = scrGirls.map(tmp => Girl(tmp._1, tmp._2, tmp._3))

//    方式一调用
//    val func1 = girls.sortBy(x => x, false)

    // 方式二：隐式转换调用
    import MyPreDef.girlToOrdered
    val func2 = girls.sortBy(x => x, false)
    println(func2.collect.toBuffer)
    sc.stop()
  }

}