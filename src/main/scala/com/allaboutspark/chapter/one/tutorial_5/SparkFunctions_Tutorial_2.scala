package com.allaboutspark.chapter.one.tutorial_5

object SparkFunctions_Tutorial_2 extends App with Context{
  val donuts = Seq(("Plain Donut",Array(1.50,2.00)),("Vanilla Donut",Array(2.00,2.50)),("Glazed Donut",Array(2.50,3.50)))
  val dfDonuts = sparkSession.createDataFrame(donuts).toDF("DonutName","Price")
  //denormalising the Array values inside the Seq
  import sparkSession.sqlContext.implicits._
  val dfDonutdenormalised = dfDonuts.select(
    $"DonutName" ,
    $"Price"(0).as("Low_Price"),
    $"Price"(1).as("High_Price")
  )
  dfDonutdenormalised.show()
}
