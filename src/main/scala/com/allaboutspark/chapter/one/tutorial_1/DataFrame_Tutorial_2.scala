package com.allaboutspark.chapter.one.tutorial_1

object DataFrame_Tutorial_2 extends App with Context {
  val dfTags=sparkSession
    .read
    .option("inferSchema",value=true)
    .option("header",value=true)
    .option("dateFormat","yyyy-MM-dd HH:mm:ss")
    .csv("D:\\Study_Materials\\Scala\\AllAboutSpark\\src\\main\\resources\\questions_10k.csv")
    .toDF("Id","CreationDate","ClosedDate","DeletionDate","Score","OwnerUserId","AnswerCount")
  dfTags.show(10)
  dfTags.printSchema()
  val dftags_CSV=dfTags.select(
    dfTags.col("Id").cast("integer"),
    dfTags.col("CreationDate").cast("timestamp"),
    dfTags.col("ClosedDate").cast("timestamp"),
    dfTags.col("DeletionDate").cast("date"),
    dfTags.col("Score").cast("integer"),
    dfTags.col("OwnerUserId").cast("integer"),
    dfTags.col("AnswerCount").cast("integer")
  )
  dftags_CSV.printSchema()


}
