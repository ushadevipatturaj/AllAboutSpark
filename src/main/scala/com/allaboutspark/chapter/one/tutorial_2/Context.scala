package com.allaboutspark.chapter.one.tutorial_2

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait Context {

  lazy val sparkConf:SparkConf = sparkConf.setMaster("local[4]").setAppName("AllAboutSpark").set("spark.cores.max","2")
  lazy val sparkSession:SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

}
