package org.example.operator
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

object map_2 {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("map_2")

    val sc = new SparkContext(sparkConf)

    val lines = sc.textFile("D:\\0x04_development\\spark_learning\\spark_learning\\data")

    val urls = lines.map(line => line.split(" ")(3))

    urls.collect().foreach(println)

  }

}
