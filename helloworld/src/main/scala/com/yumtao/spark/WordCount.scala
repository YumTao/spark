package com.yumtao.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yumtao on 2019/1/2.
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    // 创建SparkConf()并设置App名称
    val conf = new SparkConf().setAppName("WC")
    // 创建SparkContext，该对象是提交spark App的入口
    val sc = new SparkContext(conf)
    sc.textFile(args(0)).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).sortBy(_._2, false).saveAsTextFile(args(1))
    sc.stop()
  }
}
