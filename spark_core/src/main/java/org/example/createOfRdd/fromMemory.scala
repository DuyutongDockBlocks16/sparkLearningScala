package org.example.createOfRdd
import org.apache.spark. {SparkConf,SparkContext}
import org.apache.spark.rdd.RDD

object fromMemory {

  def main(args: Array[String]): Unit = {

    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("fromMemory")

    val sc = new SparkContext(SparkConf)

    val seq = Seq[Int](1,2,3,4)

//    val rdd:RDD[Int] = sc.parallelize(seq)
    val rdd:RDD[Int] = sc.makeRDD(seq)

    rdd.collect().foreach(println)

    sc.stop()


  }

}
