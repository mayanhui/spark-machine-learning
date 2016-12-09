package com.shendu.spark.graphx.prepare

import com.alibaba.fastjson.JSON
import java.io.PrintWriter
import scala.io.Source
import java.io.File

object JsonParser {

  def main(args: Array[String]) {
    
    val weiboIn = Source.fromFile("/home/zkpk/Desktop/weibo-10k-SN-data.json")
    val weiboOut = new PrintWriter(new File("/home/zkpk/Desktop/weibo-10k-SNA.json" ))
    
    for (line <- weiboIn.getLines){ 
      //println(line)
      val json = JSON.parseObject(line)
      //获取成员
      val id = json.get("id")
      val ids = json.getJSONArray("ids")
      println(id + " " + ids)
      for(i <- 0 until ids.size()){
        println(id + "\t" + ids.get(i))
        weiboOut.println(id + "\t" + ids.get(i))
      }
      
    }
    
    weiboIn.close
    weiboOut.close()    
   
  }
}