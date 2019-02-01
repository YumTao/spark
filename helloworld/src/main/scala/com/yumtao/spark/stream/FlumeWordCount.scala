package com.yumtao.spark.stream

import java.net.InetSocketAddress

import com.yumtao.utils.LoggerLevels
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yumtao on 2019/2/1.
  */
object FlumeWordCount {
  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    val conf = new SparkConf().setAppName("Flume-SparkStreaming").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Duration(5000))

    // 有状态转换时必须设置checkpoint
    ssc.checkpoint("c://ck2")

    /** Creates an input stream */
    // 方式一：spark poll 方式获取flume 数据
    // val inputStream = getFlumeInputStreamByPoll(ssc)

    // 方式二: flume push数据到spark
    val inputStream = getFlumeInputStreamByPush(ssc)

    // 获取的数据流
    val contentStream = inputStream.map(tmp => new String(tmp.event.getBody.array()))
    val result = contentStream.flatMap(_.split(" ")).map((_, 1)).updateStateByKey(updateFunc)
    result.print()
    result.saveAsTextFiles("/usr/local/tao/test_dir/spark/result/flume-wordcount", "txt")

    ssc.start()
    ssc.awaitTermination()
  }

  val updateFunc = (currentVal: Seq[Int], preVal: Option[Int]) => {
    val currentCounts: Int = currentVal.sum
    val preCounts = preVal.getOrElse(0)
    Option(currentCounts + preCounts)
  }

  /**
    * spark 主动poll方式获取flume数据
    *
    * @param ssc
    * @return
    */
  def getFlumeInputStreamByPoll(ssc: StreamingContext): ReceiverInputDStream[SparkFlumeEvent] = {
    val inputStream: ReceiverInputDStream[SparkFlumeEvent] = FlumeUtils.createPollingStream(ssc, "yumtao", 8888)
    // 多数据源使用
    //    val sources = Seq(new InetSocketAddress("yumtao", 8888), new InetSocketAddress("yumtao", 8889))
    //    val inputStream: ReceiverInputDStream[SparkFlumeEvent] = FlumeUtils.createPollingStream(ssc, sources, StorageLevel.MEMORY_AND_DISK_SER_2)

    inputStream
  }

  /**
    * flume 主动push，将数据给spark
    *
    * @param ssc
    * @return
    */
  def getFlumeInputStreamByPush(ssc: StreamingContext): ReceiverInputDStream[SparkFlumeEvent] = {
    val inputStream: ReceiverInputDStream[SparkFlumeEvent] = FlumeUtils.createStream(ssc, "singlenode", 55555)
    inputStream
  }

}
