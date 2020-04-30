package com.allaboutspark.chapter.one.tutorial_2

object SparkSQL_Joins extends App with Context {
  val dfQuestions=sparkSession.read
    .option("header",value = true)
    .option("inferSchema",value = true)
    .option("dateFormat","yyyy-MM-dd HH:mm:ss")
    .csv("D:\\Study_Materials\\Scala\\AllAboutSpark\\src\\main\\resources\\questions_10k.csv")
    .toDF()
  val dfQuestions_Reformat=dfQuestions.select(
    dfQuestions.col("Id").cast("integer"),
    dfQuestions.col("CreationDate").cast("timestamp"),
    dfQuestions.col("ClosedDate").cast("timestamp"),
    dfQuestions.col("DeletionDate").cast("date"),
    dfQuestions.col("Score").cast("integer"),
    dfQuestions.col("OwnerUserId").cast("integer"),
    dfQuestions.col("AnswerCount").cast("integer")
  )
  val dfQuestion_Subset=dfQuestions.filter("id > 400 and id < 410")
  dfQuestion_Subset.createOrReplaceTempView("temp_view_questions")

  val dftags=sparkSession
    .read
    .option("header",value = true)
    .option("inferSchema",value = true)
    .csv("D:\\Study_Materials\\Scala\\AllAboutSpark\\src\\main\\resources\\question_tags_10k.csv")
    .toDF("Id","tag")
  dftags.createOrReplaceTempView("temp_view_tags")

  //SQL Inner Join
  sparkSession.sql("select t.*,q.* from temp_view_tags t inner join temp_view_questions q on t.Id==q.Id").show(10)

  //SQL Left Join
  sparkSession.sql("select t.*,q.* from temp_view_tags t left outer join temp_view_questions q on t.Id==q.Id").show(10)

  //SQL Right Outer Join
  sparkSession.sql("select t.*,q.* from temp_view_tags t right outer join temp_view_questions q on t.Id==q.Id").show(10)

  //sql distinct
  sparkSession.sql("select distinct tag from temp_view_tags").show(10)

  //UDF
  def prefixStackOverflow(s:String):String=s"So_$s"
  //udf registration
  sparkSession.udf.register("prefixreg",prefixStackOverflow _)
  //applying udf on the temp view
  sparkSession.sql("select tag,prefixreg(tag) from temp_view_tags").show(10)
}
