package com.yumtao.spark.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yumtao on 2019/1/24.
  */
object RDDDebug {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDDDebug").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val wcRDD =
      // textFile: 1.读取文件到HadoopRDD[K,V];2.取HadoopRDD的V组成MapPartitionsRDD
      sc.textFile("hdfs://singlenode:9000//20190124/wc.txt")

      // flatMap: MapPartitionsRDD -> MapPartitionsRDD
      .flatMap(_.split(" "))

      // flatMap: MapPartitionsRDD -> MapPartitionsRDD
      .map(tmp => (tmp.trim, 1))

      // reduceByKey: 要经过shuffer,MapPartitionsRDD -> ShuffledRDD.宽依赖操作
      .reduceByKey(_ + _)


    // 打印rdd 转换流程, 结果如下，根据宽依赖划分为两个Stage.
    println(wcRDD.toDebugString)
"""
  |(2) ShuffledRDD[4] at reduceByKey at RDDDebug.scala:13 []
  | +-(2) MapPartitionsRDD[3] at map at RDDDebug.scala:13 []
  |    |  MapPartitionsRDD[2] at flatMap at RDDDebug.scala:13 []
  |    |  MapPartitionsRDD[1] at textFile at RDDDebug.scala:13 []
  |    |  hdfs://singlenode:9000//20190124/wc.txt HadoopRDD[0] at textFile at RDDDebug.scala:13 []
""".stripMargin

    sc.stop()
  }
}
