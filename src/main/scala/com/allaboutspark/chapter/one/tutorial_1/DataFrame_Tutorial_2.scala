package com.allaboutspark.chapter.one.tutorial_1

object DataFrame_Tutorial_2 extends App with Context {
   val dfTagsQuestion=sparkSession
    .read
    .option("inferSchema",value=true)
    .option("header",value=true)
    .option("dateFormat","yyyy-MM-dd HH:mm:ss")
    .csv("D:\\Study_Materials\\Scala\\AllAboutSpark\\src\\main\\resources\\questions_10k.csv")
    .toDF("Id","CreationDate","ClosedDate","DeletionDate","Score","OwnerUserId","AnswerCount")
  dfTagsQuestion.show(10)
  dfTagsQuestion.printSchema()
  val dftags_CSV=dfTagsQuestion.select(
    dfTagsQuestion.col("Id").cast("integer"),
    dfTagsQuestion.col("CreationDate").cast("timestamp"),
    dfTagsQuestion.col("ClosedDate").cast("timestamp"),
    dfTagsQuestion.col("DeletionDate").cast("date"),
    dfTagsQuestion.col("Score").cast("integer"),
    dfTagsQuestion.col("OwnerUserId").cast("integer"),
    dfTagsQuestion.col("AnswerCount").cast("integer")
  )
  dftags_CSV.printSchema()
  val scoreCount= dftags_CSV.filter("score>400 and score<=410").count()
  println(s"score count between 400 to 410 = $scoreCount")

}
