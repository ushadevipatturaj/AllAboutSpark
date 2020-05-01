package com.allaboutspark.chapter.one.tutorial_3
import org.apache.spark.sql.functions._
object SparkStatistics_Tutorial_1 extends App with Context {
  val dfTags = sparkSession.read
    .option("inferSchema", value = true)
    .option("header", value = true)
    .csv("D:\\Study_Materials\\Scala\\AllAboutSpark\\src\\main\\resources\\question_tags_10k.csv")
    .toDF("Id", "tag")

  val dfTagQuestion = sparkSession.read
    .option("inferSchema", value = true)
    .option("header", value = true)
    .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
    .csv("D:\\Study_Materials\\Scala\\AllAboutSpark\\src\\main\\resources\\questions_10k.csv")
    .toDF("Id", "CreationDate", "ClosedDate", "DeletionDate", "Score", "OwnerUserId", "AnswerCount")

  val dfTagQuestion_Formatted = dfTagQuestion.select(
    dfTagQuestion.col("Id").cast("integer"),
    dfTagQuestion.col("CreationDate").cast("timestamp"),
    dfTagQuestion.col("ClosedDate").cast("timestamp"),
    dfTagQuestion.col("DeletionDate").cast("date"),
    dfTagQuestion.col("Score").cast("integer"),
    dfTagQuestion.col("OwnerUserId").cast("integer"),
    dfTagQuestion.col("AnswerCount").cast("integer")
  )
  dfTagQuestion_Formatted.select(avg("score")).show(10,truncate = false)
  //dfTagQuestion_Formatted.select("Id").groupBy("Id").count().filter("count >1").show(10)
  dfTagQuestion_Formatted.select( avg("score"),min("score"),max("score"),mean("score"),sum("score")).show()

  //Groupby with statistics
  dfTagQuestion_Formatted.filter("Id > 400 and Id <=450").filter("OwnerUserId is not null")
    .join(dfTags, dfTagQuestion_Formatted.col("Id").equalTo(dfTags("Id")))
    .groupBy(dfTagQuestion_Formatted.col("Id"))
    .agg(avg("score"),max("AnswerCount")).show()

  //statistics using describe
  val dfTag_Describe = dfTagQuestion_Formatted.describe()
  dfTag_Describe.show()

  //correlation
  val dfTagCorrelation = dfTagQuestion_Formatted.stat.corr("Score","AnswerCount")
  println(s"Correlation between score and AnswerCount is  $dfTagCorrelation")

  //covariance
  val dfTagCovariance = dfTagQuestion_Formatted.stat.cov("Score","AnswerCount")
  println(s"Covariance between score and AnswerCount is  $dfTagCovariance")

  //frequent Items
  val dfTagsFreq = dfTagQuestion_Formatted.stat.freqItems(Seq("Score"))
  dfTagsFreq.show(10)

  //crosstab
  val dfTagsCrossTab = dfTagQuestion_Formatted.filter("OwnerUserId>1 and OwnerUserId<20")
    .stat.crosstab("Score","OwnerUserId")
  dfTagsCrossTab.show()


}