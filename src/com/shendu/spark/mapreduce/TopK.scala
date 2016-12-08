package com.shendu.spark.mapreduce

import org.apache.spark._
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object TopK {
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val line = sc.textFile(args(0))
    val result = line.flatMap(_.split("\\s+")).map((_, 1)).reduceByKey(_+_)
    val sorted = result.map{case(key,value) => (value,key)}.sortByKey(true,1)
    val topk = sorted.top(args(1).toInt)
    topk.foreach(println)
    sc.stop
  }
}