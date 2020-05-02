package com.allaboutspark.chapter.one.tutorial_4

import org.apache.spark.sql.Dataset

object DataframeOperations extends App with Context {
  val dfTags = sparkSession.read
    .option("inferSchema",value = true)
    .option("header",value = true)
    .csv("D:\\Study_Materials\\Scala\\AllAboutSpark\\src\\main\\resources\\question_tags_10k.csv")
    .toDF("Id","tag")

  val dfTagQuestions = sparkSession.read
    .option("inferSchema",value = true)
    .option("header",value = true)
    .csv("D:\\Study_Materials\\Scala\\AllAboutSpark\\src\\main\\resources\\questions_10k.csv")
    .toDF("Id", "CreationDate", "ClosedDate", "DeletionDate", "Score", "OwnerUserId", "AnswerCount")

  val dfTagQuestion_Formatted = dfTagQuestions.select(
    dfTagQuestions.col("Id").cast("integer"),
    dfTagQuestions.col("CreationDate").cast("timestamp"),
    dfTagQuestions.col("ClosedDate").cast("timestamp"),
    dfTagQuestions.col("DeletionDate").cast("date"),
    dfTagQuestions.col("Score").cast("integer"),
    dfTagQuestions.col("OwnerUserId").cast("integer"),
    dfTagQuestions.col("AnswerCount").cast("integer")
  )
  val dfTagQuestionFiltered = dfTagQuestion_Formatted.filter("Score > 400 and Score <410").join(dfTags,"Id")
    .select("OwnerUserId", "tag", "CreationDate", "Score").toDF()
  dfTagQuestionFiltered.show(10)
  import sparkSession.implicits._
  case class Tag(Id:Int, tag: String)
  val dfTagsofTags: Dataset[Tag] = dfTags.as[Tag]
  dfTagsofTags.take(10).foreach(t => println(s"Id is ${t.Id} and Tag is ${t.tag}"))
}
