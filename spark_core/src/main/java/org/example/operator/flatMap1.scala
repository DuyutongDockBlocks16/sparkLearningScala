package org.example.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object flatMap1 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("flatMap");

    val sc = new SparkContext(sparkConf);

    val lists_rdd:RDD[Any] = sc.makeRDD(List[Any](List(1, 2), 3,List(3, 4)), 2)

    val flatRDD = lists_rdd.flatMap(
      data=>{
        data match {
          case list:List[_]=>list
          case dat=>List(dat)
        }
      }
    )

    flatRDD.collect().foreach(println)

  }

}
