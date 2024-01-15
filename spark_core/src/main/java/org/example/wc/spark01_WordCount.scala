package org.example.wc
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark01_WordCount {
  def main(args: Array[String]): Unit = {
//    Application

//    Spark框架

//    TODO 建立和spark框架的链接
//    JDBC: Connection
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
//    TODO 执行业务操作
//    1.读取文件，获取一行一行的数据
//    hello，world
//    val line:RDD[String] = sc.textFile("D:/0x04_development/spark_learning/spark_learning/data_wc")

    val line:RDD[String] = sc.textFile("/mnt/d/0x04_development/spark_learning/spark_learning/data_wc")

    val groupWords:RDD[(String,Int)] = line.flatMap(x => x.split(" ")).map(x=>(x,1))

    val value = groupWords.reduceByKey((x,y)=>{x+y})

    val result:Array[(String,Int)]= value.collect()
    result.foreach(println)


//    2.将一行数据进行拆分，进行分词效果

//    3.将数据根据单词进行分组

//    TODO 关闭连接
    sc.stop()
  }
}
