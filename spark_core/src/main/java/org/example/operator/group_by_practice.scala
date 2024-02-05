package org.example.operator
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object group_by_practice {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("group_by_practice")
    val sc = new SparkContext(sparkConf)

    val rdd1:RDD[String] = sc.textFile("D:\\0x04_development\\spark_learning\\spark_learning\\data")
//    rdd1.collect().foreach(println)
    val groupedRDD: RDD[(String, Iterable[(String, Int)])] = rdd1.map { string => (string.substring(8, 17), 1)}.groupBy {case (time, _) => time}
    val groupedRDD2:RDD[(String, Iterable[Int])] = groupedRDD.map { case (key, values) =>
      ( key,values.map(value => value._2))
    }

    groupedRDD2.map{case (k,v) =>{
      (k,v.sum)
    }}.collect().foreach(println)

//    groupedRDD2.collect().foreach(println)

    sc.stop()



  }

}
