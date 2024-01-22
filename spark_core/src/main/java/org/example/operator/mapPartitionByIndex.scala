package org.example.operator
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

object mapPartitionByIndex {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("mapPartitionByIndex")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1,2,3,4,5,6,7,8),2)

    val mapiRDD: RDD[Int] = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        if (index == 1) {
          iter
        } else {
          Nil.iterator
        }

      }
    )

    mapiRDD.collect().foreach(println)


  }




}
