package ece454

import org.apache.spark.{SparkContext, SparkConf}

object Task2 {
  def lineSum(line: String): Int = {
      line.split(",").drop(1).foldLeft(0) {
        (acc, x) => 
          if (x != "") (acc+1)
          else acc
      }
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Task1 Application")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    val count = List(
        textFile
        .map(line => lineSum(line))
        .sum
        .toInt
        )
    sc.makeRDD(count).saveAsTextFile(args(1))
  }
}
