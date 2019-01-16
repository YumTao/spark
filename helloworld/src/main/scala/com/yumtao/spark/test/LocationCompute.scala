package com.yumtao.spark.test

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yumtao on 2019/1/15.
  */
object LocationCompute {
  def main(args: Array[String]): Unit = {
    val param = "9F36407EAD0629FC166F14DDE7970F68 | A0D76858FF50828223285A4F8B1B63EA | 116.29891 | 39.88928 | 0 | 0 | 0 | 47 |  | 1 |  |  |  | 2 | 2"
    println(param.split("\\|").length)

    val csMsg = "1D24705B14FD55D2A0F9C4C87539C06A|347D3A57FB4A1461605FA463EE25FC3E|460|01|939A3F98A55859912C9260612E75C4F5|3E1953B572576CC82887D4100A29A02C|3C9B5F1CD6A030BCB7BED9EAC80589FB|2015-11-19 06:24:52.926008|41210|17121|2015-11-19 06:24:53.635831|1|32|011|19|201511"
    println(csMsg.split("\\|").length)


    val localtionFile = args(0)
    val cellFile = args(1)

    // 创建SparkConf()并设置App名称
    val conf = new SparkConf().setAppName("WC")
    // 创建SparkContext，该对象是提交spark App的入口
    val sc = new SparkContext(conf)
    val locationGp = sc.textFile(localtionFile).map(_.split("\\|"))
    val homeGp = locationGp.filter(x => isHome(x(7)))
    val companyGp = locationGp.filter(x => isCompany(x(7)))

    val lacMap = sc.textFile(cellFile).map(_.split("\\|")).map(_.map(_.trim)).map(tmp => (tmp(0), tmp))


    val tmp1 = "9F36407EAD0629FC166F14DDE7970F68 | A0D76858FF50828223285A4F8B1B63EA | 116.29891 | 39.88928 | 0 | 0 | 0 | 47 |  | 1 |  |  |  | 2 | 2"
    val tmp2 = "9F36407EAD0629FC166F14DDE7970F68 | 7CBB793C46D7CAD3981035CF922BD3F1 | 116.31401 | 39.89521 | 0 | 0 | 0 | 45 |  | 1 |  |  |  | 2 | 2"


    //    sc.stop()
  }

  def isHome(lacTime: String): Boolean = {
    return getLocationTypeFromTime(lacTime) == 1
  }

  def isCompany(lacTime: String): Boolean = {
    return getLocationTypeFromTime(lacTime) == 2
  }

  private def getLocationTypeFromTime(lacTime: String): Int = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    val time = sdf.parse(lacTime)
    val calendar = Calendar.getInstance()
    calendar.setTime(time)
    val hour = calendar.get(Calendar.HOUR_OF_DAY)
    if (hour > 9 && hour < 18) {
      return 1
    } else {
      return 2
    }
  }


}
