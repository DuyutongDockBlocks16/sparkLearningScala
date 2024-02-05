package org.example.operator
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object group_by1 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("groupby1")
    val sc = new SparkContext(sparkConf)

    val rdd1:RDD[String] = sc.makeRDD(List[String]("List", "Lime", "Hadoop", "Hive"), 1)

    rdd1.map(string => string+"1").collect().foreach(println)

    def groupByFunc(string: String) = {
      string.headOption.getOrElse("").toString
    }

    rdd1.groupBy(groupByFunc).map{
      case (key, values) => {
        (key,values.map(value=>value+key))
      }
    }.collect().foreach(println)

    sc.stop()

  }

}
