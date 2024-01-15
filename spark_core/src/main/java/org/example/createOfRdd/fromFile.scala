package org.example.createOfRdd
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext,SparkConf}

object fromFile {

  def main(args: Array[String]): Unit = {
    val SparkConf = new SparkConf().setMaster("local").setAppName("fromFile")
    val sc = new SparkContext(SparkConf)

//    val rdd:RDD[String] = sc.textFile("D:\\0x04_development\\spark_learning\\spark_learning\\spark_core\\src\\main\\java\\org\\example\\createOfRdd")
//    也可以是目录名


//    也可以是HDFS路径
    val rdd:RDD[String] = sc.textFile("hdfs://linux1:8082/test1/txt").repartition(2)

//    路径为元组的第一个元素，文件中的内容为元组中的第二个元素
    val rdd1:RDD[(String, String)] = sc.wholeTextFiles("hdfs://linux1:8082/test1/txt")

    rdd.collect().foreach(println)


    sc.stop()
  }

}
