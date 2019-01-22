package com.yumtao.spark.sql

import java.util.Properties

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yumtao on 2019/1/22.
  */
object SinkToDB {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SinkToDB").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val personRDD = sc.parallelize(Array(Person(3, "guoguo", 26), Person(4, "niu", 26)))
    val df = personRDD.toDF

    // DB properties
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "*******")

    // 写入数据到DB, model:append(追加)，更多可点进mode方法查看
    df.write.mode("append").jdbc(
      "jdbc:mysql://singlenode:3306/spark?characterEncoding=utf-8",
      "person", prop
    )

    // 从DB中读取数据
    val personDF = sqlContext.read.format("jdbc").options(Map(
      "url" -> "jdbc:mysql://singlenode:3306/spark?characterEncoding=utf-8",
      "driver" -> "com.mysql.jdbc.Driver",
      "dbtable" -> "person",
      "user" -> "root",
      "password" -> "*******"
    )).load
    personDF.show

    sc.stop()
  }
}
