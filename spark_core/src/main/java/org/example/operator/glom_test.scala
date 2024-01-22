package org.example.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object glom_test {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("glom")
    val sc = new SparkContext(sparkConf)

    val rdd:RDD[Int] = sc.makeRDD(List[Int](1, 2, 3, 4),2)

    val rdd_2: RDD[Array[Int]] = rdd.glom()

    val max_rdd: Int = rdd_2.map(
      array => {
        array.max
      }
    ).collect().sum

    println(max_rdd)


  }

}
