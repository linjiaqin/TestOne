package com.ljq

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}

object  WordCountOne {
  def main(args: Array[String])= {
    //sparkconf用来初始化spark的相关配置
    val conf = new SparkConf().setAppName("wordcount").setMaster("local")
    //val conf = new SparkConf().setAppName("wordcount")
      //.setMaster("spark://127.0.0.1:7077")
    //sparkcontext是spark的入口，用来初始话调度器，task等，是spark最重要的对象
    val sc = new SparkContext(conf)
    sc.addJar("/home/linjiaqin/code/IdeaProjects/TestOne/out/artifacts/TestOne/TestOne.jar")
    val hdfs = "hdfs://localhost:9000"
    val input = hdfs + "/user/linjiaqin/wordcounttext"
    val output = hdfs + "/laorenyuhaioutput1"


    val text = sc.textFile(input)
    val count = text.flatMap(x => x.split(" ") ).map(x => (x,1)).reduceByKey((x,y) => x+y)

    //count.collect().foreach(print)
    text.mapPartitionsWithIndex{
      (partIdx,iter) => {
        var part_map = scala.collection.mutable.Map[String,Int]()
        while(iter.hasNext){
          var part_name = "part_" + partIdx;
          if(part_map.contains(part_name)) {
            var ele_cnt = part_map(part_name)
            part_map(part_name) = ele_cnt + 1
          } else {
            part_map(part_name) = 1
          }
          iter.next()
        }
        part_map.iterator
      }
    }.collect.foreach(println)
    count.saveAsTextFile(output)



  }
}

