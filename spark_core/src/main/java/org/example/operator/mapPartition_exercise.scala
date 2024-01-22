package org.example.operator
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

object mapPartition_exercise {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("mapP_exe").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1,2,3,4,5,6,7,9),2)

    val mapMax:RDD[Int] = rdd.mapPartitions(
      iter =>{
        List(iter.max).iterator
      }
    )

  }

}
