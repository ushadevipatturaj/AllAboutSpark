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
  //where & and condition
  sparkSession.sql("select * from temp_view where tag like 'c%' and (Id=403 or Id=409) ").show(10)
  //In clause
  sparkSession.sql("select * from temp_view where Id in (403,409)").show()
  //groupby
  sparkSession.sql("select tag,count(tag) from temp_view group by tag").show(10)
  //orderby
  sparkSession.sql("select tag,count(*) as count from temp_view group by tag order by count desc").show(10)
  //group by & having
  sparkSession.sql("select tag,count(tag) as count from temp_view group by tag having count>5 order by count ").show(10)
}
