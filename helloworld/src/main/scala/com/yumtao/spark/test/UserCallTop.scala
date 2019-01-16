package com.yumtao.spark.test

import java.text.SimpleDateFormat

import com.yumtao.spark.util.DateUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yumtao on 2019/1/16.
  */
object UserCallTop {
  val sdf = new SimpleDateFormat("yyyyMMddHHmmss")

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UserCallTop").setMaster("local")
    val sc = new SparkContext(conf)

    // ArrayBuffer((18688888888,16030401EAFB68F1E3CDF819735E1C66,30960000),
    // (18611132889,16030401EAFB68F1E3CDF819735E1C66,34500000))
    val topCall = getTopCallByPhone(sc)

    // 将cellId做为key (cellId, (phone, callTime))
    // ArrayBuffer((16030401EAFB68F1E3CDF819735E1C66,(18688888888,30960000)),
    // (16030401EAFB68F1E3CDF819735E1C66,(18611132889,34500000)))
    val topCallByCellId = topCall.map(tmp => {
      (tmp._2, (tmp._1, tmp._3))
    })
    println("user location: " + topCallByCellId.collect.toBuffer)

    // 获取位置信息(cellId, (latitude, longtitude, high))
    // ArrayBuffer((9F36407EAD0629FC166F14DDE7970F68,(116.304864,40.050645,6)),
    // (CC0710CC94ECC657A8561DE549D940E0,(116.303955,40.041935,6)),
    // (16030401EAFB68F1E3CDF819735E1C66,(116.296302,40.032296,6)))
    val location = sc.textFile("D:/tmp/spark/lac_info.txt").map(_.split(",")).map(array => {
      val cellId = array(0)
      val latitude = array(1)
      val longtitude = array(2)
      val high = array(3)
      (cellId, (latitude, longtitude, high))
    })
    println("cell msg: " + location.collect.toBuffer)

    // 用户通话时间最长位置与位置信息内连，获取用户通话时间最长的位置信息
    // ArrayBuffer((16030401EAFB68F1E3CDF819735E1C66,((18688888888,30960000),(116.296302,40.032296,6))),
    // (16030401EAFB68F1E3CDF819735E1C66,((18611132889,34500000),(116.296302,40.032296,6))))
    val joinVal = topCallByCellId.join(location)
    println("joinVal: " + joinVal.collect.toBuffer)

    // 结果处理
    // ArrayBuffer((18688888888,(116.296302,40.032296),30960000), (18611132889,(116.296302,40.032296),34500000))
    val topCallTimeLocation = joinVal.map(tmp => {
      val cellId = tmp._1
      val phone = tmp._2._1._1
      val callTime = tmp._2._1._2
      val latitude = tmp._2._2._1
      val longtitude = tmp._2._2._2
      (phone, (latitude, longtitude), callTime)
    })
    println("result msg: " + topCallTimeLocation.collect.toBuffer)

    // 结果保存
    topCallTimeLocation.saveAsTextFile("D:/tmp/spark/location_out")
    sc.stop()
  }

  private def getTopCallByPhone(sc: SparkContext) = {
    /**
      * 1. 统计每个手机号在每个基站ID的通话时长
      */
    // 1.1 每个手机号在每个基站ID，通话时间点
    // (手机号+基站id, 时间毫秒值, 通话类型)
    // ArrayBuffer((18688888888&16030401EAFB68F1E3CDF819735E1C66,1459038240000,1))
    val callNodeByPhoneAndCellId = sc.textFile("D:/tmp/spark/bs_log").map(line => line.split(",")).map(
      lineArray => {
        val phone = lineArray(0)
        val cellId = lineArray(2)
        val callType = lineArray(3)
        val absTime = DateUtils.getTime(lineArray(1), "yyyyMMddHHmmss")
        (s"$phone&$cellId", absTime, callType)
      }
    )
    println("用户位置通话节点绝对值：" + callNodeByPhoneAndCellId.collect.toBuffer)

    // 1.2 每个手机号在每个基站ID，通话时间点(发起时间为-，结束时间为正)
    // ArrayBuffer((18688888888&16030401EAFB68F1E3CDF819735E1C66,-1459038240000))
    val callNodeByPhoneAndCellIdAndType = callNodeByPhoneAndCellId.map(tmp => {
      val phoneAndCellId = tmp._1
      val absTime = tmp._2
      val callType = tmp._3
      val time = if (callType.toInt == 0) absTime else -absTime
      (phoneAndCellId, time)
    })
    println("用户位置通话节点：" + callNodeByPhoneAndCellIdAndType.collect.toBuffer)

    // 1.3 每个手机号在每个基站ID，通话时长
    val callTimeByPhoneAndCellId = callNodeByPhoneAndCellIdAndType.reduceByKey((leftTime: Long, rightTime: Long) => leftTime + rightTime)
    println("用户位置通话时长:" + callTimeByPhoneAndCellId.collect.toBuffer)

    /**
      * 2. 获取每个用户通话时长最长的信息
      */
    val topCallTimeByPhone = callTimeByPhoneAndCellId.groupBy(
      // 2.1. 根据用户分组
      tmp => {
        val phoneAndCellId = tmp._1.split("&")
        val phone = phoneAndCellId(0)
        phone
      }).mapValues(iter => {
      // 2.2. 每个用户的通话时长集合按通话时长倒序排列取第一个
      iter.toList.maxBy(_._2)
    }).map(
      // 2.3 封装结果格式(phone, cellId, callTime)
      tmp => {
        val phone = tmp._1
        val cellId = tmp._2._1.split("&")(1)
        val callTime = tmp._2._2
        (phone, cellId, callTime)
      }
    )
    println("topCallRdd: " + topCallTimeByPhone.collect.toBuffer)

    topCallTimeByPhone
  }



}
