package org.example.operator
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

object distinct {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("distinct")
    val sc = new SparkContext(sparkConf)

    val values: RDD[Int] = sc.makeRDD(List[Int](1, 1, 2, 2, 3, 4, 4, 5))

    values.distinct().collect().foreach(println)

  }

}
