package com.allaboutspark.chapter.one.tutorial_1

object DataFrame_Joins extends App with Context {
  val dftags=sparkSession
    .read
    .option("header",value = true)
    .option("inferSchema",value = true)
    .csv("D:\\Study_Materials\\Scala\\AllAboutSpark\\src\\main\\resources\\question_tags_10k.csv")
    .toDF("Id","tag")
  val dfTagsQuestion=sparkSession
    .read
    .option("inferSchema",value=true)
    .option("header",value=true)
    .option("dateFormat","yyyy-MM-dd HH:mm:ss")
    .csv("D:\\Study_Materials\\Scala\\AllAboutSpark\\src\\main\\resources\\questions_10k.csv")
    .toDF("Id","CreationDate","ClosedDate","DeletionDate","Score","OwnerUserId","AnswerCount")
  val dftags_CSV=dfTagsQuestion.select(
    dfTagsQuestion.col("Id").cast("integer"),
    dfTagsQuestion.col("CreationDate").cast("timestamp"),
    dfTagsQuestion.col("ClosedDate").cast("timestamp"),
    dfTagsQuestion.col("DeletionDate").cast("date"),
    dfTagsQuestion.col("Score").cast("integer"),
    dfTagsQuestion.col("OwnerUserId").cast("integer"),
    dfTagsQuestion.col("AnswerCount").cast("integer")
  )

  val dftagCsvSubset=dftags_CSV.filter("score>400 and score<=410")

  //Joining 2 dataframes

  val joinedDF=dftagCsvSubset.join(dftags,"Id")
  joinedDF.show()

  //Print selected columns
  println("Printing the selected columns")
  joinedDF.select("Id","CreationDate","Score","AnswerCount","tag" ).show()

  //Join explicit column name and this prints ID column of both dataframes.
  println("Joining using explict column names")
  val joinExplicitDF=dftagCsvSubset.join(dftags,dftagCsvSubset("Id") === dftags("Id"))
  joinExplicitDF.show(false)

  //inner join
  println("Inner Join:")
  val innerJoinDF1=dftagCsvSubset.join(dftags,dftagCsvSubset("Id") === dftags("Id"),"inner")
  val innerJoinDF2=dftagCsvSubset.join(dftags,Seq("Id"),"inner")
  innerJoinDF1.show()
  innerJoinDF2.show()

  //left, left_semi and left_anti
  val leftjoinDF=dftagCsvSubset.join(dftags,Seq("Id"),"left")
  val leftsemiDF=dftagCsvSubset.join(dftags,Seq("Id"),"left_semi")
  val leftantiDF=dftagCsvSubset.join(dftags,Seq("Id"),"left_anti")
  println("Left Join")
  leftjoinDF.show()
  println("Left Semi")
  leftsemiDF.show()
  println("Left Anti")
  leftantiDF.show()

}
