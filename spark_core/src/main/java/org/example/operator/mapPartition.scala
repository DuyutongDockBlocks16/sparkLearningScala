package org.example.operator
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

object mapPartition {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("mapPartitions")
    val sc = new SparkContext(sparkConf);

    val rdd = sc.makeRDD(List(1,2,3,4),2)

    val mapRdd: RDD[Int] = rdd.mapPartitions(
      iter => {
        println(">>>>>>>>>>>")


        iter.map(_ * 2)
      }
    )

    mapRdd.collect().foreach(println)
  }





}
