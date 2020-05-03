package com.allaboutspark.chapter.one.tutorial_5

object SparkFunctions_Tutorial_1 extends App with Context {
  val donuts:Seq[(String,Double)] = Seq(("plain donut", 1.50), ("vanilla donut", 2.0), ("glazed donut", 2.50))
  val dfDonuts = sparkSession.createDataFrame(donuts).toDF("Donut Name","Price")
  dfDonuts.show()

  //Getting column names from dataframe

  val dfColumns:Array[String] = dfDonuts.columns
  dfColumns.foreach(println(_))


}
