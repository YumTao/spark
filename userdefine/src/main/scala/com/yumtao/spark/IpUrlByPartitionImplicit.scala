package com.yumtao.spark

import com.yumtao.utils.FileUtils
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  * 自定义分区-通过隐式转换
  */
object MyPreDef2 {
  implicit def myUrlPartition(obj: MyUrl): Partitioner = {
    new Partitioner {
      override def numPartitions: Int = obj.ips.size

      override def getPartition(key: Any): Int = {
        val seqList = obj.ips.toList
        key match {
          case key: String => if (seqList.contains(key)) seqList.indexOf(key) else 0
          case _ => 0
        }
      }
    }
  }
}

object IpUrlByPartitionImplicit {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("IpUrlStatistic").setMaster("local[30]")
    val sc = new SparkContext(conf)

    // 2019-01-17 00:13:24.565 INFO [cn.yqt.springmvc.web.filter.ForCrawlerFilter.doFilter:58] access ip: 45.62.249.171, url: /public/index.php
    val accessLine = sc.textFile(FileUtils.getPathInProject("yumtao/console.log")).filter(_.contains("access ip:"))
    val ip2Url = accessLine.map(getIp2Url)
    println(ip2Url.collect.toBuffer)

    val keys = ip2Url.keys.collect.toSet
    println(keys.size)
    import MyPreDef2.myUrlPartition
    ip2Url.partitionBy(MyUrl(keys)).saveAsTextFile(FileUtils.getPathInProject("mypartition"))

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

case class MyUrl(val ips: Set[String])
