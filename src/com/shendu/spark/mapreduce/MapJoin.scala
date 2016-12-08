package com.shendu.spark.mapreduce

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object MapJoin {
  
  def main(args: Array[String]): Unit = {
    var sparkConf = new SparkConf().setAppName("map-side-join")
    var sc = new SparkContext(sparkConf)
    var table1 = sc.textFile(args(0))
    var table2 = sc.textFile(args(1))

    // table1 is smaller, so broadcast it as a map<String, String>
    var pairs = table1.map {
      x =>
        var pos = x.indexOf(',')
        (x.substring(0, pos), x.substring(pos + 1))
    }.collectAsMap
    var broadCastMap = sc.broadcast(pairs) //save table1 as map, and broadcast it

    // table2 join table1 in map side
    var result = table2.map {
      x =>
        var pos = x.indexOf(',')
        (x.substring(0, pos), x.substring(pos + 1))
    }.mapPartitions({
      iter =>
        var m = broadCastMap.value
        for {
          (key, value) <- iter
          if (m.contains(key))
        } yield (key, (value, m.get(key).getOrElse("")))
    })

    result.saveAsTextFile(args(2)) //save result to local file or HDFS

  }

}