package com.yumtao.utils

import java.text.SimpleDateFormat
import java.util.Calendar

/**
  * Created by yumtao on 2019/1/16.
  */
object DateUtils {

  def getTime(dateStr: String, format: String): Long = {
    val sdf = new SimpleDateFormat(format)
    sdf.parse(dateStr).getTime
  }

  def format(millionTime: Long, format: String): String = {
    var now = Calendar.getInstance()
    now.setTimeInMillis(millionTime)
    val date = now.getTime
    val sdf = new SimpleDateFormat(format)
    sdf.format(date)
  }
}
