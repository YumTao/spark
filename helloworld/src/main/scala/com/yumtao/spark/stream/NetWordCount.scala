package com.yumtao.spark.stream

import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yumtao on 2019/1/28.
  */
object NetWordCount {
  def main(args: Array[String]): Unit = {
    /* 1. 创建StreamingContext， 注册输入源，获取数据 */
    // 注意：Spark Streaming 程序,本地模式excutor数必须 >= 2, 因为需要receiver和数据处理两个线程
    // cpu core >= 2, 才能做到数据接收与数据处理并行
    val conf = new SparkConf().setAppName("NET_WC").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // 获取StreamingContext对象, 每2秒将读入数据divided一次
    val ssc = new StreamingContext(sc, Duration(2000))

    // 注册输入源, 返回值为读入数据divided后的DStream
    val lines1 = ssc.socketTextStream("singlenode", 55555)

    val lines2 = ssc.socketTextStream("singlenode", 55556)


    /* 2. 数据处理 */
    // 单词统计代码
    val word2Count = lines1.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    word2Count.print()

    val word2Count2 = lines2.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    word2Count2.print()

    /* 3. 开始计算 */
    ssc.start()
    // 不关闭程序，保持运行
    ssc.awaitTermination()
  }
}
