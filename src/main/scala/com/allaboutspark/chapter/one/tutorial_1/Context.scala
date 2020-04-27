package com.allaboutspark.chapter.one.tutorial_1

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait Context {
  lazy val sparkConf=new SparkConf().setMaster("Local[*]").setAppName("AllAboutSpark").set("spark.cores.max","2")
  lazy val sparkSession=SparkSession.builder().config(sparkConf).getOrCreate()

}
