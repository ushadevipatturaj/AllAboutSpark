package com.allaboutspark.chapter.one.tutorial_4

import java.time.ZonedDateTime

import org.apache.spark.sql.functions._
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
  val dfTagQuestionFiltered = dfTagQuestions.filter("Score > 400 and Score <410").join(dfTags,"Id")
    .select("OwnerUserId", "tag", "CreationDate", "Score").toDF()
  //dfTagQuestionFiltered.show(10)
  dfTagQuestionFiltered.printSchema()

  //Creating a dataset from dataframe
  import sparkSession.implicits._
  case class Tag(Id:Int, tag: String)
  val dfTagsofTags: Dataset[Tag] = dfTags.as[Tag]
  dfTagsofTags.take(10).foreach(t => println(s"Id is ${t.Id} and Tag is ${t.tag}"))

  //row level parsing, applying map on row level

  case class Questions (OwnerUserId: Int, tag: String, CreationDate: java.sql.Timestamp, Score: Int)

  def toQuestions(row: org.apache.spark.sql.Row) = {
    val IntOf : String => Option[Int] = {
      case str if str == "NA" => None
      case str => Some(str.toInt)
    }

    val DateOf : String => java.sql.Timestamp = {
      case str => java.sql.Timestamp.valueOf(ZonedDateTime.parse(str).toLocalDateTime)
    }

    Questions(
      OwnerUserId = IntOf(row.getString(0)).getOrElse(-1),
      tag = row.getString(1),
      CreationDate = row.getTimestamp(2),
      Score = row.getInt(3)
    )
  }

  val dfQuestionDataset: Dataset[Questions] = dfTagQuestionFiltered.map(row => toQuestions(row))
  dfQuestionDataset.foreach(row => println(s"owner userid = ${row.OwnerUserId}, tag = ${row.tag}, creation date = ${row.CreationDate}, score = ${row.Score}"))

  //Creating dataframe from collections
  val seqData = Seq(
    1 -> "1_One",
    2 -> "2_Two",
    3-> "3_Three"
  )

  val SeqDF = seqData.toDF("Id","Tag")
  SeqDF.show(10)

  //Union of 2 dataframes
  val dfUnion = dfTags.union(SeqDF)
  dfUnion.filter("Id >=1 and Id<6").show(25)

  //Intersection of 2 dataframes
  val dfIntersection = dfUnion.intersect(SeqDF)
  dfIntersection.show(10)

  val dfSplit = SeqDF.withColumn("temp", split($"Tag","_"))
    .select(
      $"Id",
      $"Tag",
      $"temp".getItem(0).as("Alias Name"),
      $"temp".getItem(1).as("Numerical value")
    ).drop("temp")
  dfSplit.show()

}
