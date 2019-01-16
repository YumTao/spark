package com.yumtao.spark.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yumtao on 2019/1/7.
  */
object RDDDemo {
  def main(args: Array[String]): Unit = {
    // 创建SparkConf()并设置App名称
    val conf = new SparkConf().setAppName("WC").setMaster("spark://singlenode:7077")
    // 创建SparkContext，该对象是提交spark App的入口
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9), 1)


    /**
      * mapPartitionsWithIndex(index :Int, iter: Iterator[T] ): Iterator[U]
      *
      * @param index : 分片id
      * @param iter  : 分片所存储的数据集
      * @return 处理后的数据集
      */
    rdd1.mapPartitionsWithIndex((index, iter) => iter.toList.map(x => s"(iter=$index,val=$x)").iterator)

    /**
      * 聚合:先在各个分区汇总，再各个分区汇总结果再进行汇总
      * aggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): U
      */
    rdd1.aggregate(0)(_ + _, _ + _)

    val pairRDD = sc.parallelize(List(("cat", 2), ("cat", 5), ("mouse", 4), ("cat", 12), ("dog", 12), ("mouse", 2)), 2)
    pairRDD.aggregateByKey(0)(_ + _, _ + _)


    /**
      * checkpoint:快照
      * 步骤：
      * 1. 设置checkPointDir：sc.setCheckpointDir("hdfs://singlenode:9000/ck")
      * 2. 执行checkpoint操作：rdd.checkpoint
      * 注意：
      * - checkpoint也是transformation算子，只有在执行对应action后才会执行该算子。
      * - checkpoint执行成功了,前面所有的RDD依赖都会被销毁，所以一般在checkpoint运算前会将对应rdd进行缓存 rdd.cache
      */
    sc.setCheckpointDir("hdfs://singlenode:9000/ck")
    val rdd = sc.parallelize(1 to 1000)
    rdd.checkpoint
    rdd.isCheckpointed // 检查是否执行了checkpoint

    /**
      * 分区操作：
      * 1. coalesce: 合并分区
      * 2. repartition: 重新分区
      */
    val partDemoRdd = sc.parallelize(1 to 1000, 10)
    partDemoRdd.coalesce(2)
    partDemoRdd.repartition(100)

    /**
      * collectAsMap: collect + toMap
      */
    sc.parallelize(List("a" -> 1, "b" -> 1, "a" -> 3)).collectAsMap()

    /**
      * combineByKey同reduceByKey
      * 第一个参数x:原封不动取出来, 第二个参数:是函数, 局部运算, 第三个:是函数, 对局部运算后的结果再做运算
      */
    val combineByKeyRdd = sc.parallelize(List("a" -> 1, "b" -> 1, "a" -> 3))
    combineByKeyRdd.combineByKey(x => x, (seqVal1: Int, seqVal2: Int) => seqVal1 + seqVal2, (goalVal1: Int, goalVal2: Int) => goalVal1 + goalVal2)

    /**
      * countByKey :根据key count
      * countByValue : 根据元素 count
      */
    val countRdd = sc.parallelize(Array("a" -> 1, "b" -> 1, "a" -> 10, "a" -> 1))
    countRdd.countByKey // Map(b -> 1, a -> 3)
    countRdd.countByValue // Map((b,1) -> 1, (a,10) -> 1, (a,1) -> 2)

    /**
      * filterByRange(lower, upper) 筛选出[lower, upper]范围的列表，左右包含
      */
    val filterRdd = sc.parallelize(List(("e", 5), ("c", 3), ("d", 4), ("c", 2), ("b", 1), ("a", 1)))
    filterRdd.filterByRange("b", "c").collect

    /**
      * flatMapValues: key不动，对value进行flatMap重新与key组合
      */
    val ftMapValRdd = sc.parallelize(List(("a", "1 2"), ("b", "3 4")))
    ftMapValRdd.flatMapValues(_.split(" ")).collect

    /**
      * foldByKey(defaultValue)(fun2(x,y)): 根据key聚合value
      * defaultValue: 与value操作的初始值(与value类型一致)
      * fun2(x,y): 对value的操作
      */
    val foldByKeyRdd = sc.parallelize(List("dog", "wolf", "cat", "bear"))
    val length2WordRdd = foldByKeyRdd.map(x => (x.length, x)) // Array((3,dog), (4,wolf), (3,cat), (4,bear))
    length2WordRdd.foldByKey("")(_ + _) // Array((4,wolfbear), (3,dogcat))

    val iterParttion = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 3)
    iterParttion.foreachPartition(x => println(x))

    /**
      * keyBy : 以传入的参数做key与当前元素组成新的tuple
      */
    val keyByRdd = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
    keyByRdd.keyBy(_.length).collect

    /**
      * join:内连 以key进行join，相同key的在同一集合内
      * leftOuterJoin 左外联
      * rightOuterJoin 右外联
      */
    val leftRdd = sc.parallelize(Array("a" -> 1, "c" -> 1))
    val rightRdd = sc.parallelize(Array("a" -> 2, "b" -> 2))
    leftRdd.join(rightRdd) // Array((a,(1,2)))
    leftRdd.leftOuterJoin(rightRdd) // Array((a,(1,Some(2))), (c,(1,None)))
    leftRdd.rightOuterJoin(rightRdd) // Array((b,(None,2)), (a,(Some(1),2)))
  }

}
