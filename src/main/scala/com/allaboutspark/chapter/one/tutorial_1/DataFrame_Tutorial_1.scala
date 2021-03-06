package com.allaboutspark.chapter.one.tutorial_1
object DataFrame_Tutorial_1 extends Context with App{
val dfTags=sparkSession
  .read
  .option("header",value = true)
  .option("inferSchema",value = true)
  .csv("D:\\Study_Materials\\Scala\\AllAboutSpark\\src\\main\\resources\\question_tags_10k.csv")
  .toDF("id","tag")

  dfTags.show(10,truncate = false)
  dfTags.printSchema()
  dfTags.select("tag").show(10) //printing the specific columns of the data frame
  dfTags.filter("tag=='osx'").show(25)
  val countcsv:Long=dfTags.filter("tag=='osx'").count()
  println("Total count of osx is "+countcsv)

  //SQL like Query
  dfTags.filter("tag like 's%'").show(10)

  //multi filter chaining
  dfTags.filter("tag like 'o%'").filter("id==469 or id==8970").show()

  //SQL IN Clause
  dfTags.filter("id in (4,2900)").show()

  //SQL Groupby
  dfTags.groupBy("tag").count().show(10)

  //SQL Groupby with filter
  dfTags.groupBy("tag").count().filter("count>5").show(10)

  //SQL Orderby
  dfTags.groupBy("tag").count().filter("count>5").orderBy("count").show(10)
}
