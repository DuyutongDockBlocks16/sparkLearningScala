package org.example.operator
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

object sample {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sample")
    val sc = new SparkContext(sparkConf)

    val raw_data: RDD[Int] = sc.makeRDD(List[Int](1, 2, 3, 4, 5, 6, 7))

    println(raw_data.sample(
      true,0.4,1
    ).collect().mkString(","))


  }

}
