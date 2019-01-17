package com.yumtao.spark

import com.yumtao.utils.FileUtils
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  * @goal 分析www.yumtao.com博客日志, 获取访问ip与url, 将不同的ip写入至不同的文件中
  * @src resource 下yumtao/console.log
  * @author Created by yumtao on 2019/1/17.
  */
object IpUrlStatistic {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:/software/hadoop-2.6.0-cdh5.7.0")

    val conf = new SparkConf().setAppName("IpUrlStatistic").setMaster("local[30]")
    val sc = new SparkContext(conf)

    // 2019-01-17 00:13:24.565 INFO [cn.yqt.springmvc.web.filter.ForCrawlerFilter.doFilter:58] access ip: 45.62.249.171, url: /public/index.php
    val accessLine = sc.textFile(FileUtils.getPathInProject("yumtao/console.log")).filter(_.contains("access ip:"))
    val ip2Url = accessLine.map(getIp2Url)
    println(ip2Url.collect.toBuffer)

    val keys = ip2Url.keys.collect.toSet
    println(keys.size)
    ip2Url.partitionBy(new MyPartitioner(keys)).saveAsTextFile(FileUtils.getPathInProject("mypartition"))

    sc.stop()
  }

  def getIp2Url(line: String): (String, String) = {
    val subLine = line.substring(line.indexOf("access ip:"))
    val subLineArray = subLine.split(",")
    val ip = subLineArray(0).split(":")(1).trim
    val url = subLineArray(1).split(":")(1).trim
    (ip, url)
  }
}


/**
  * 自定义分区
  * 1. 继承Partitioner, 实现numPartitions, getPartition方法
  * 2. numPartitions: 返回总分区数
  * 3. getPartition: 传入key,自定义逻辑返回对应分区号(0 -- 分区数减一)
  *
  * @param ips
  */
class MyPartitioner(ips: Set[String]) extends Partitioner {
  val seqList = ips.toList

  override def numPartitions: Int = ips.size

  override def getPartition(key: Any): Int = {
    key match {
      case key: String => if (seqList.contains(key)) seqList.indexOf(key) else 0
      case _ => 0
    }
  }
}
