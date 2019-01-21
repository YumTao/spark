package com.yumtao.ipfrom

import com.yumtao.utils.{FileUtils, IpUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 根据ip规则数据, 统计访客ip的所在位置
  * Created by yumtao on 2019/1/21.
  */
object CustomerIpLocation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MyIpLocation").setMaster("local[3]")
    val sc = new SparkContext(conf)
    /**
      * 1. 加载ip规则数据封装成Array((start_num, end_num, province, city),...)
      * 2. 加载访客ip数据,获取每个访客ip的十进制
      * 3. 遍历访客ip数据,查找访客ip十进制在ip规则集合的start_num, end_num中，返回对应规则信息
      */

    /* 1.封装ip规则集 */
    val ipRuleRdd = getIpRule(sc, FileUtils.getPathInProject("ipfrom/ip.txt"))

    // 广播规则数据
    val ipRuleBroadcast = sc.broadcast(ipRuleRdd.collect)

    /* 2.获取访客ip集  */
    val accessIpsRdd = getAccessIps(sc, FileUtils.getPathInProject("ipfrom/access.log"))

    /* 3.遍历访客ip, 二分查找匹配的规则数据,返回访客ip所在地信息 */
    val accessIPLocationMsg = accessIpsRdd.map(ip => {
      getLocationMsgFromRule(ipRuleBroadcast, ip)
    })
    println("access ip location msg: " + accessIPLocationMsg.collect.toBuffer)

    sc.stop()
  }

  /**
    * 读取文件,返回ip规则RDD
    * @return RDD[(ip, startNum, endNum, ip, province, city)]
    */
  def getIpRule(sc: SparkContext, filePath: String) = {
    // 1.0.1.0|1.0.3.255|16777472|16778239|亚洲|中国|福建|福州||电信|350100|China|CN|119.306239|26.075302
    sc.textFile(filePath).map(
      // 按|切分
      line => line.split("\\|")
    ).map(
      // 返回(start_num, end_num, province, city)
      array => {
        val start_num = array(2)
        val end_num = array(3)
        val province = array(6)
        val city = array(7)
        (start_num, end_num, province, city)
      }
    )
  }

  /**
    * 读取文件,返回访客ip RDD
    * @return RDD[ip]
    */
  def getAccessIps(sc: SparkContext, filePath: String) = {
    // 20090121000132095572000|125.213.100.123|show.51.com|/shoplist.php?phpfile=shoplist2.php&style=1&sex=137|Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; Mozilla/4.0(Compatible Mozilla/4.0(Compatible-EmbeddedWB 14.59 http://bsalsa.com/ EmbeddedWB- 14.59  from: http://bsalsa.com/ )|http://show.51.com/main.php|
    sc.textFile(filePath).map(
      line => line.split("\\|")
    ).map(
      array => {
        val accessIp = array(1)
        accessIp
      }
    )
  }

  /**
    * 传入ip,根据规则集返回ip位置信息
    * @return
    */
  def getLocationMsgFromRule(ipRuleBroadcast: Broadcast[Array[(String, String, String, String)]], ip: String) = {
    val ipLongVal = IpUtils.ip2Long(ip)
    val ipRule = ipRuleBroadcast.value
    val index = binarySearch(ipRule, ipLongVal)
    if (index != -1) {
      val iPLocationMsg = ipRule(index)
      val ipProvince = iPLocationMsg._3
      val ipCity = iPLocationMsg._4
      (ip, ipProvince, ipCity)
    } else {
      (ip, "未知", "未知")
    }
  }

  /**
    * 二分查找
    * 传入ip的Long值，返回对应匹配规则数据的index
    */
  def binarySearch(lines: Array[(String, String, String, String)], ip: Long): Int = {
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      if ((ip >= lines(middle)._1.toLong) && (ip <= lines(middle)._2.toLong))
        return middle
      if (ip < lines(middle)._1.toLong)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    -1
  }
}
