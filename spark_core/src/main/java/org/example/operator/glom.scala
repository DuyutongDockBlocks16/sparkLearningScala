package org.example.operator
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

object glom {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("glom")
    val sc = new SparkContext(sparkConf)

    val rdd:RDD[Int] = sc.makeRDD(List[Int](1, 2, 3, 4))

    val rdd_2: RDD[Array[Int]] = rdd.glom()

    rdd_2.collect().foreach(data=>println(data.mkString(",")))


  }

}
