package com.allaboutspark.chapter.one.tutorial_2

object SparkSQL_Tutorial extends App with Context {
  val dfTags=sparkSession.read
    .option("inferSchema",true)
    .option("header",true)
    .csv("D:\\Study_Materials\\Scala\\AllAboutSpark\\src\\main\\resources\\question_tags_10k.csv")
    .toDF()
}
