package com.shendu.spark.mapreduce

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object ReduceJoin {
  
  def main(args: Array[String]): Unit = {
    var sparkConf = new SparkConf().setAppName("reduce-side-join")
    var sc = new SparkContext(sparkConf)
    var table1 = sc.textFile(args(0))
    var table2 = sc.textFile(args(1))
    var pairs = table1.map { x =>
      var pos = x.indexOf(',')
      (x.substring(0, pos), x.substring(pos + 1))
    }
    var result = table2.map { x =>
      var pos = x.indexOf(',')
      (x.substring(0, pos), x.substring(pos + 1))
    }.join(pairs)
    result.saveAsTextFile(args(2))

  }
}