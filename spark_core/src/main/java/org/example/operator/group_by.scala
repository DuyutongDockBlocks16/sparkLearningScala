package org.example.operator
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

object group_by {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("group_by")
    val sc = new SparkContext(sparkConf)

    val rdd1:RDD[Int] = sc.makeRDD(List[Int](1, 2, 3, 4), 2)

    def groupByFunc(num:Int) = {
      num % 2
    }

    rdd1.groupBy(groupByFunc).collect().foreach(println)

  }

}
