package com.yumtao.ipfrom

import com.yumtao.utils.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 比对两系统交易流水号，找出不同部分。
  * Created by yumtao on 2019/1/29.
  */
object DrawBalance {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DrawBalance").setMaster("local")
    val sc = new SparkContext(conf)

    /**
      * 1. 获取我方系统wbs提现成功的流水号与金额rdd1
      * 2. 获取第三方ftp提现成功的流水号与金额rdd2
      * 3. 数据对比，获取对账不同数据
      */
    // wbs 提现成功rdd1(流水号, 金额)
    val wbsNo2Amt = sc.textFile(FileUtils.getPathInProject("draw/wbs_draw_success.txt"))
      .map(_.split("\t"))
      .map(tmp => DrawObj(tmp(6), BigDecimal(tmp(1)).*(BigDecimal(100)).toInt))

    // ftp提现成功rdd2(流水号, 金额)
    val ftpNo2Amt: RDD[DrawObj] = getDrawRddFromFtp(sc)

    // wbs - ftp，获取我方系统成功，第三方系统不成功的数据
    val wbsCutFtp = wbsNo2Amt.subtract(ftpNo2Amt)
    wbsCutFtp.map(_.thirdNo).saveAsTextFile(FileUtils.getPathInProject("draw/result/wbsCutFtp"))

    val wbsCutFtpAmt = wbsCutFtp.map(tmp => BigDecimal(tmp.amt.toString)./(BigDecimal(100))).sum()
    println(s"wbsCutFtp总金额$wbsCutFtpAmt")


    // ftp - wbs,获取第三方系统成功，我方系统不成功的数据
    val ftpCutWbs = ftpNo2Amt.subtract(wbsNo2Amt)
    ftpCutWbs.map(_.thirdNo).saveAsTextFile(FileUtils.getPathInProject("draw/result/ftpCutWbs"))

    val ftpCutWbsAmt = ftpCutWbs.map(tmp => BigDecimal(tmp.amt.toString)./(BigDecimal(100))).sum()
    println(s"ftpCutWbs总金额$ftpCutWbsAmt")

    sc.stop()
  }

  /**
    * 读取ftp文件，获取提现成功的rdd
    */
  private def getDrawRddFromFtp(sc: SparkContext) = {
    sc.textFile(FileUtils.getPathInProject("draw/WTTX20190128.txt"))
      .map(_.split("\\|"))
      .map(tmp => {
        try {
          Option(tmp(2), tmp(5), tmp(9))
        } catch {
          case e: Exception => None
        }
      })
      .filter(option => option.isInstanceOf[Some[(String, String, String)]]).map(_.get)
      .filter(_._3.equals("0000")).map(tmp => DrawObj(tmp._1, tmp._2.toInt))
  }
}


case class DrawObj(val thirdNo: String, val amt: Int) extends Serializable