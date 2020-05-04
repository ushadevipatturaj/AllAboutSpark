package com.allaboutspark.chapter.one.tutorial_5
import org.apache.spark.sql.functions._
object SparkFunctions_Tutorial_1 extends App with Context {
  val donuts:Seq[(String,Double)] = Seq(("plain donut", 1.50), ("vanilla donut", 2.0), ("glazed donut", 2.50))
  val dfDonuts = sparkSession.createDataFrame(donuts).toDF("Donut Name","Price")
  dfDonuts.show()

  //Getting column names from dataframe

  val dfColumns:Array[String] = dfDonuts.columns
  dfColumns.foreach(println(_))

  //getting the datatypes and column names using dtypes
  val (columnNames,columnDatatype) = dfDonuts.dtypes.unzip
  println(s"Column Names are :${columnNames.mkString(",")}")
  println(s"Column Datatypes are :${columnDatatype.mkString(",")}")

  //Reading a json file
  import sparkSession.sqlContext.implicits._
  val dfJSON = sparkSession.read
    .option("inferSchema",value = true)
    .option("multiLine", value = true)
    .json("D:\\Study_Materials\\Scala\\AllAboutSpark\\src\\main\\resources\\tags_sample.json")

  val dfExplode = dfJSON.select(explode($"stackoverflow") as "StackOverflow")
  dfExplode.printSchema()

  dfExplode.select(
    $"StackOverflow.tag.Id" as "Id",
    $"StackOverflow.tag.author" as "Author",
    $"StackOverflow.tag.frameworks.id" as "Framework Id",
    $"StackOverflow.tag.frameworks.name" as "Framework Name",
    $"StackOverflow.tag.name" as "Tag Name"
  ).show(truncate = false)

}
