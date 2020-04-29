package com.allaboutspark.chapter.one.tutorial_1
object DataFrame_Tutorial extends Context with App{
val dfTags=sparkSession
  .read
  .option("header",value = true)
  .option("inferSchema",value = true)
  .csv("D:\\Study_Materials\\Scala\\AllAboutSpark\\src\\main\\resources\\question_tags_10k.csv")
  .toDF("id","tag")

  dfTags.show(10,truncate = false)
  val countcsv:Long=dfTags.count()
  println("Total count is "+countcsv)
  dfTags.printSchema()


}
