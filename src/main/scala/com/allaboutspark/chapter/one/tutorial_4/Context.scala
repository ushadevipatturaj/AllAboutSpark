package com.allaboutspark.chapter.one.tutorial_4

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait Context {
  lazy val sparkConf:SparkConf = new SparkConf().setMaster("local[4]").setAppName("AllABoutSpark").set("spark.cores.max","2")
  lazy val sparkSession:SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

}
