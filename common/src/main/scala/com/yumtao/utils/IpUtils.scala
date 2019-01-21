package com.yumtao.utils

/**
  * Created by yumtao on 2019/1/21.
  */
object IpUtils {
  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }
}
