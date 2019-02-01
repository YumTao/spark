package com.yumtao.spark.stream

import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 监听singlenode服务器55555端口发送的数据，统计其数据的单词出现次数
  * 统计单词出现次数，为全局统计。要用到有状态转换
  * Created by yumtao on 2019/1/28.
  */
object NetWordCountGlobal {
  def main(args: Array[String]): Unit = {
    /* 1. 创建StreamingContext， 注册输入源，获取数据 */
    // 注意：Spark Streaming 程序,本地模式excutor数必须 >= 2, 因为需要receiver和数据处理两个线程
    // cpu core >= 2, 才能做到数据接收与数据处理并行
    val conf = new SparkConf().setAppName("NET_WC").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // 获取StreamingContext对象, 每2秒将读入数据divided一次
    val ssc = new StreamingContext(sc, Duration(5000))
    ssc.checkpoint("c://ck2")

    // 注册输入源, 返回值为读入数据divided后的DStream
    val lines = ssc.socketTextStream("singlenode", 55555)

    /* 2. 数据处理 */
    // 单词统计代码
    val word2Count = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).updateStateByKey(updateFunction)
    word2Count.print()


    /* 3. 开始计算 */
    ssc.start()
    // 不关闭程序，保持运行
    ssc.awaitTermination()
  }


  /**
    * 状态更新函数
    *
    * @param currentValues 当前时间的RDD内相同key的value列表
    * @param preValues     之前的value值
    * @return
    */
  def updateFunction(currentValues: Seq[Int], preValues: Option[Int]): Option[Int] = {
    val curr = currentValues.sum //seq列表中所有value求和
    val pre = preValues.getOrElse(0) //获取上一状态值
    Some(curr + pre)
  }

}
