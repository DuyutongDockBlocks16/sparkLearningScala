package org.example.operator
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

object partition_not_change {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("partition_not_change")
    var sc = new SparkContext(sparkConf)

    val rdd1:RDD[Int] = sc.makeRDD(List[Int](1, 2, 3, 4), 2)
    rdd1.saveAsTextFile("output1")

    rdd1.map(_*2).saveAsTextFile("output2")

    sc.stop()



  }

}
