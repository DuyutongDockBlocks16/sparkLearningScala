package org.example.createOfRdd
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}


object fromMemoryPartitionSet {

  def main(args: Array[String]): Unit = {
    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("setPartitions")
    SparkConf.set("spark.default.parallelism","5")

    val sc = new SparkContext(SparkConf)

    val list = List[Int](1,2,3,4)

    sc.makeRDD(list,numSlices = 2).saveAsTextFile("output")
    sc.stop()

  }


}
