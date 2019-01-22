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
    sparkSqlL(personDF)

    // 2. 使用SQL进行操作数据
//    sql(personDF, sqlContext)

    // 以json方式输出到文件中
    personDF.write.json(FileUtils.getPathInProject("sql/out/person_json"))
    sc.stop()
  }

  /**
    * SparkSql API查询
    *
    * @param df
    */
  private def sparkSqlL(df: DataFrame) = {
    // select id from table
    df.select("id").show
    // select * from table where id < 3 AND age = 25
    df.filter("id < 3 AND age = 25").show

    // 内联查询
    // select t1.*, t2.* from table t1 inner join table t2 on t1.id = t2.id
    df.join(df, df("id") === df("id"), "inner").show

    // 聚合函数
    // select age, count(age) from table group by age
    df.groupBy(df("age")).count.show

    // 分页
    df.limit(2).show

    // 打印scheme
    df.printSchema
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