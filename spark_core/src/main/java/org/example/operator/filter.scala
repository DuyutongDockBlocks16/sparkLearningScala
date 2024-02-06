package org.example.operator
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
object filter {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("filter")
    val sc = new SparkContext(sparkConf)

    val number_rdd:RDD[Int] = sc.makeRDD(List[Int](1, 2, 3, 4, 4, 5, 6))

    val even_numbers:RDD[Int] = number_rdd.filter { number => number % 2 == 0 }

    even_numbers.collect().foreach(println)

    sc.stop()

  }
}
