package com.yumtao.spark.sql

import com.yumtao.utils.FileUtils
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yumtao on 2019/1/18.
  */
object HelloWorld {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SqlHelloWorld").setMaster("local")
    val sc = new SparkContext(conf)
    val personRDD = sc.textFile(FileUtils.getPathInProject("sql/helloworld.txt"))
      .map(_.split("\t")).map(x => Person(x(0).toInt, x(1), x(2).toInt))
    println(personRDD.collect.toBuffer)

    // 创建SQLContext
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val personDF = personRDD.toDF
    personDF.show

    // 1. 使用SparkSql API操作数据
    //    sparkSqlL(personDF)

    // 2. 使用SQL进行操作数据
    sql(personDF, sqlContext)


    sc.stop()
  }

  /**
    * SparkSql API查询
    *
    * @param df
    */
  private def sparkSqlL(df: DataFrame) = {
    df.select("id").show
    df.filter("id < 3 AND age = 25").show
  }

  /**
    * 使用sql 操作数据
    *
    * @param df
    */
  private def sql(df: DataFrame, sqlContext: SQLContext): Unit = {
    // 注册表
    df.registerTempTable("t_person")

    // 传入SQL
    val resultDF = sqlContext.sql("select * from t_person where id < 2")
    resultDF.show

    // 内连查询
    val joinDF = sqlContext.sql("SELECT t1.*, t2.* FROM t_person t1 INNER JOIN t_person t2 ON t1.id = t2.id")
    joinDF.show
  }
}

case class Person(id: Int, name: String, age: Int) extends Serializable
