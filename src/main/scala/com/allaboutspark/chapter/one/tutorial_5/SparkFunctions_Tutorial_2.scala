package com.allaboutspark.chapter.one.tutorial_5
import org.apache.spark.sql.functions._
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

  //renaming a column name
  val dfrenamed = dfDonutdenormalised.withColumnRenamed("DonutName","Donut's Name")
  dfrenamed.show()

  //Adding constant columns to the dataframes
  val dfWithNewColumns = dfDonutdenormalised.withColumn("Tasty", lit("Very Tasty"))
    .withColumn("Correlation" , lit(1))
    .withColumn("Stock Min, Max", typedLit(Seq(100,500)))
  dfWithNewColumns.show()

  //creating and applying udf for the dataframe
  val stockMinMax:String => Seq[Int] = donutname => donutname match {
    case "Plain Donut" => Seq(100,500)
    case "Vanilla Donut" => Seq(200,400)
    case "Glazed Donut" => Seq(100,300)
    case _ => Seq(150,150)
  }

  //udf registration for the method
  val stockVariable = udf(stockMinMax)
  val dfUDF = dfDonutdenormalised.withColumn("Tasty",lit("Very Tasty"))
    .withColumn("Stock Min Max",stockVariable($"DonutName"))
  dfUDF.show()

  //extracting the first row and its columns
  val donuts1 = Seq(("Plain Donut",2.00),("Vanilla Donut",2.50),("Glazed Donut",3.50))
  val dfDonuts1 = sparkSession.createDataFrame(donuts1).toDF("DonutName","Price")
  val firstRow = dfDonuts1.first()
  println(s"First row of the data frame is $firstRow")
  val firstColumn = dfDonuts1.first().get(0)
  println(s"First column value of the first row is $firstColumn")
  val secondColumn = dfDonuts1.first().getAs[Double]("Price")
  println(s"First column value of the second row is $secondColumn")

  //Formatting the columns
  val donuts2 = Seq(("Plain Donut",2.00,"2020-04-05"),("Vanilla Donut",2.50,"2020-05-05"),("Glazed Donut",3.50,"2020-12-05"))
  val dfDonuts2 = sparkSession.createDataFrame(donuts2)
    .toDF("DonutName","Price","Date")
  val dfDonutFormatted = dfDonuts2.withColumn("Formatted_Price", format_number($"Price",2))
    .withColumn("Formatted_String", format_string("New %s", $"DonutName"))
    .withColumn("Upper_Donut",upper($"DonutName"))
    .withColumn("Lower_Donut",lower($"DonutName"))
    .withColumn("Date_Formatted",date_format(to_date($"Date","yyyy-dd-MM"),"yyyyMMdd"))
    .withColumn("Day_of_Purchase",dayofmonth($"Date"))
    .withColumn("Month_of_Purchase",month($"Date"))
    .withColumn("Year_of_Purchase",year($"Date"))

  dfDonutFormatted.show()

  //Hashing a column
  val dfHashing = dfDonuts2.withColumn("Hash",hash($"DonutName"))
    .withColumn("MD5",md5($"DonutName"))
    .withColumn("SHA1",sha1($"DonutName"))
    .withColumn("SHA2",sha2($"DonutName",256))

  dfHashing.show(truncate = false)
  //DataFrame String Operations
  val dfStringFunctions = dfDonuts2.withColumn("Instr" ,instr($"DonutName","Donut"))
      .withColumn("Length",length($"DonutName"))
      .withColumn("trim",trim($"DonutName"))
      .withColumn("Ltrim",ltrim($"DonutName"))
      .withColumn("Rtrim",rtrim($"DonutName"))
      .withColumn("Reverse",reverse($"DonutName"))
      .withColumn("SubString",substring($"DonutName",0,5))
      .withColumn("IsNull",isnull($"DonutName"))
      .withColumn("ConcatWs",concat_ws("-",$"DonutName",$"Price"))
      .withColumn("InitCap",$"DonutName")
  dfStringFunctions.show()

  //Drop null values
  val donuts3 = Seq(("Plain Donut",2.00),("Vanilla Donut",2.50),("Glazed Donut",3.50),(null,1.00))
  val dfDonuts_withNull = sparkSession.createDataFrame(donuts3).toDF("DonutName","Price")
  dfDonuts_withNull.show()

  val dfDonuts_withoutNull = dfDonuts_withNull.na.drop()
  dfDonuts_withoutNull.show()
}
