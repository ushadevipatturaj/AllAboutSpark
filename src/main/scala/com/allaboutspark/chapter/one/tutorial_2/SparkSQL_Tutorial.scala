package com.allaboutspark.chapter.one.tutorial_2

object SparkSQL_Tutorial extends App with Context {

  val dftags=sparkSession
    .read
    .option("header",value = true)
    .option("inferSchema",value = true)
    .csv("D:\\Study_Materials\\Scala\\AllAboutSpark\\src\\main\\resources\\question_tags_10k.csv")
    .toDF("Id","tag")
  dftags.createOrReplaceTempView("temp_view")
  //to validate whether the view has been registered with the spark session
  sparkSession.catalog.listTables().show()
  //using sql query to print all temp table created
  sparkSession.sql("show tables").show(10)
  //printing the columns using spark sql query
  sparkSession.sql("select id,tag from temp_view limit 10").show()
    //filter by column value
  sparkSession.sql("select * from temp_view where tag='osx'").show(25)
  //getting count
  sparkSession.sql("select count(*) as osx_count from temp_view where tag='osx'").show()
  //applying sql like clause
  sparkSession.sql("select * from temp_view where Id like '4%'").show(25)
}
