package com.shendu.spark.mapreduce

import org.apache.spark._
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object Distinct {
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("Distinct")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val distinct = sc.textFile(args(0))
    val result = distinct.filter(_.trim().length > 0).map(line => (line.trim, "")).
      groupByKey().sortByKey().keys.collect.foreach(println _)
    
    //result.saveAsTextFile(args(1))
  }
}