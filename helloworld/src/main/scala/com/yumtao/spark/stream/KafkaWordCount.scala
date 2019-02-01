package com.yumtao.spark.stream

import com.yumtao.utils.LoggerLevels
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * SparkStreaming 获取kafka中的数据
  * Created by root on 2016/5/21.
  */
object KafkaWordCount {

  def main(args: Array[String]) {
    LoggerLevels.setStreamingLogLevels()
    val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 有状态转换时必须设置checkpoint
    ssc.checkpoint("c://ck2")

    val topicMap = Map("spark-wc01" -> 1, "spark-wc02" -> 2)
    val zkQuorum = "singlenode:2181"
    val group = "CG-SPARK"

    /**
      * Create an input stream that pulls messages from Kafka Brokers.
      *
      * @param ssc          StreamingContext object
      * @param zkQuorum     Zookeeper quorum (hostname:port,hostname:port,..)
      * @param groupId      The group id for this consumer
      * @param topics       Map of (topic_name -> numPartitions) to consume. Each partition is consumed
      *                     in its own thread
      * @param storageLevel Storage level to use for storing the received objects
      *                     (default: StorageLevel.MEMORY_AND_DISK_SER_2)
      */
    val data = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, StorageLevel.MEMORY_AND_DISK_SER)

    val words = data.map(_._2).flatMap(_.split(" "))

    val wordCounts = words.map((_, 1)).updateStateByKey(updateFunc _, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 状态更新函数
    *
    * @param iter : Iterator[(key, Seq[Value], Option[Value])]
    *             key 为当前DStream[K,V]的key
    *             Seq[Value] 为当前DStream[K,V]key相同的value集
    *             Option[Value] 为当前DStream[K,V]key的上一次Value
    * @return Iterator[(String, Int)],计算后的key、value对
    */
  def updateFunc(iter: Iterator[(String, Seq[Int], Option[Int])]): Iterator[(String, Int)] = {
    iter.flatMap { case (x, y, z) => Some(y.sum + z.getOrElse(0)).map(i => (x, i)) }
  }

}
