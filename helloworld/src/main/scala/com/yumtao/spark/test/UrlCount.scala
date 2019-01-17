package com.yumtao.spark.test

import java.net.URI

import com.yumtao.utils.FileUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @goal 根据url访问日志，统计每个host下的访问次数多的url
  * @src 资源文件path: resource/UrlCount.log
  * @author Created by yumtao on 2019/1/16.
  */
object UrlCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UrlCount").setMaster("local")
    val sc = new SparkContext(conf)

    val path = FileUtils.getPathInProject("UrlCount.log")
    //    func1(sc, path)
    func2(sc, path)

    sc.stop()
  }

  /**
    * 存在内存溢出问题
    */
  private def func1(sc: SparkContext, filePath: String) = {
    // ArrayBuffer((url, time),(url, time))
    val url2Time = sc.textFile(filePath)
      // 切分
      .map(_.split("\t"))
      // key为url, value为time
      .map(tmp => {
      (tmp(1), tmp(0))
    })
    println("url2Time: " + url2Time.collect.toBuffer)

    // ArrayBuffer((url, count),(url, count))
    val url2Count = url2Time.mapValues(_ => 1L).reduceByKey(_ + _).sortBy(_._2, false)
    println("url2Count: " + url2Count.collect.toBuffer)

    val url2CountOrder = url2Count
      // 组装ArrayBuffer((host, (url, path, count)))
      .map(tmp => {
      val url = tmp._1
      val count = tmp._2
      val urlObj = new URI(url)
      val host = urlObj.getHost
      val path = urlObj.getPath
      (host, (url, path, count))
    })
      // 按host进行分组
      .groupBy(_._1)
      // TODO mapValues里头代码为scala API操作，不是RDD API,所以当数据量过大时，可能造成内存溢出
      .mapValues(iter => {
      // 同一host的所有集合按照访问次数排序取最后一个
      val urlCountGroup = iter.toList
      urlCountGroup.sortBy(tmp => {
        val urlPathCount = tmp._2
        urlPathCount._3
      }).reverse(0)
    })
    println("url2CountOrder: " + url2CountOrder.collect.toBuffer)

    // 结果整理
    val result = url2CountOrder.map(tmp => {
      val host = tmp._1
      val path = tmp._2._2._2
      val count = tmp._2._2._3
      (host, (path, count))
    })
    println("result: " + result.collect.toBuffer)
  }

  def func2(sc: SparkContext, filePath: String): Unit = {
    // ArrayBuffer((url, time),(url, time))
    val url2Time = sc.textFile(filePath)
      // 切分
      .map(_.split("\t"))
      // key为url, value为time
      .map(tmp => {
      (tmp(1), tmp(0))
    })
    println("url2Time: " + url2Time.collect.toBuffer)

    // ArrayBuffer((url, count),(url, count))
    val url2Count = url2Time.mapValues(_ => 1L).reduceByKey(_ + _).sortBy(_._2, false)
    println("url2Count: " + url2Count.collect.toBuffer)

    val hostArray = Array("net.itcast.cn", "java.itcast.cn", "php.itcast.cn")
    for (host <- hostArray) {
      // 筛选出当前host的RDD
      val cUrl2Count = url2Count.filter(url2CountTmp => {
        val url = url2CountTmp._1
        val urlObj = new URI(url)
        urlObj.getHost.equals(host)
      })

      val topCount = cUrl2Count.sortBy(_._2, false).first
      println(s"current host:$host, 访问次数最多的为" + topCount)
    }
  }

}
