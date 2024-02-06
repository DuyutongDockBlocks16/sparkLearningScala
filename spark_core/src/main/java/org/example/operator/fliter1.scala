package org.example.operator
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

object fliter1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("filter_1")
    val sc = new SparkContext(sparkConf)

    var raw_lines: RDD[String] = sc.textFile("D:\\0x04_development\\spark_learning\\spark_learning\\data")

    raw_lines.map{string => (string.substring(8, 17),string)}.filter{
      case (k,_) => {
        k.equals("2024:14:4")
      }
    }.collect().foreach(println)




  }

}
