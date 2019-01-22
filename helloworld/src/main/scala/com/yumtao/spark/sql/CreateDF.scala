package com.yumtao.spark.sql

import com.yumtao.utils.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * Created by yumtao on 2019/1/22.
  */
object CreateDF {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DFByStructType").setMaster("local")
    val sc = new SparkContext(conf)

    val personRDD = sc.textFile(FileUtils.getPathInProject("sql/helloworld.txt")).map(_.split("\t"))
    println(personRDD.collect.toBuffer)

    val sqlContext = new SQLContext(sc)

    /**
      * 1. 根据RDD[Row]创建DataFrame
      * createDataFrame(rowRDD: RDD[Row], schema: StructType)
      */
    //    val df: DataFrame = createByRowAndScheme(personRDD, sqlContext)

    /**
      * 2. 根据java bean创建DataFrame
      * createDataFrame(rdd: RDD[_], beanClass: Class[_])
      */
    //    val df: DataFrame = createByJavaBean(personRDD, sqlContext)

    /**
      * 3. 根据RDD[scalaObj]创建DataFrame(常用)
      * rdd.toDF
      */
    val df: DataFrame = createByScalaBean(personRDD, sqlContext)

    df.show
    sc.stop()
  }

  private def createByScalaBean(personRDD: RDD[Array[String]], sqlContext: SQLContext) = {
    import sqlContext.implicits._
    val persons = personRDD.map(tmp => Person(tmp(0).toInt, tmp(1), tmp(2).toInt))
    persons.toDF
  }

  private def createByJavaBean(personRDD: RDD[Array[String]], sqlContext: SQLContext) = {
    val persons = personRDD.map(tmp => new JPerson(tmp(0).toInt, tmp(1), tmp(2).toInt))
    val df = sqlContext.createDataFrame(persons, classOf[JPerson])
    df
  }

  private def createByRowAndScheme(personRDD: RDD[Array[String]], sqlContext: SQLContext): DataFrame = {
    val schema = StructType(
      List(
        StructField("id", IntegerType),
        StructField("name", StringType),
        StructField("age", IntegerType)
      )
    )

    val rowRDD = personRDD.map(tmp => Row(tmp(0).toInt, tmp(1), tmp(2).toInt))
    sqlContext.createDataFrame(rowRDD, schema)
  }
}
