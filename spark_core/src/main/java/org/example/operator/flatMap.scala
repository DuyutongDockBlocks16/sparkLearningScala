package org.example.operator
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

object flatMap {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("flatMap");

    val sc = new SparkContext(sparkConf);

    val lists_rdd:RDD[List[Int]] = sc.makeRDD(List[List[Int]](List(1, 2), List(3, 4)), 2)

    val new_rdd:RDD[Int] = lists_rdd.flatMap(x=>x)



  }

}
