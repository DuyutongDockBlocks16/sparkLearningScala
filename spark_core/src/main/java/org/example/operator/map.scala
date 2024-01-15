package org.example.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

object map {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("map")

    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List[Int](1,2,3,4))

    def mapFunction(num: Int): Int = {
      num * 2
    }

    val result = rdd.map(mapFunction).collect()



    result.foreach(println)

  }

}
