package org.example.operator
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

object filter2 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("filter")
    val sc = new SparkContext(sparkConf)

    val raw_lines: RDD[String] = sc.textFile("D:\\0x04_development\\spark_learning\\spark_learning\\data")

    val filtered_data: RDD[String] = raw_lines.filter { line => {
        val columns = line.split(" ")
        val method = columns(2)

        method.equals("POST")
      }
    }

    filtered_data.collect().foreach(println)



  }

}
