package com.example
package transformers

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row}

class DataConverter extends SessionWrapper {

  import spark.implicits._

  def totalTimeToFloat(df: DataFrame): Float = {
    df.take(1)(0)
      .getLong(0)
      .toFloat
  }

  private def convertToDoubleDigit(str: String): String = {
    val regex = """\d""".r
    str match {
      case regex() => s"0$str"
      case _ => str
    }
  }

  def convertToDateForm(date: Array[Row]): String = {

    val year = date(0)
      .getInt(0)
      .toString
    val month = date(0)
      .getInt(1)
      .toString
    val day = date(0)
      .getInt(2)
      .toString

    val dateForm = s"$year.${convertToDoubleDigit(month)}.${convertToDoubleDigit(day)}"
    dateForm

  }

  def createMetaInfo(collected: String, processed: String): DataFrame = {

    val data = Seq((collected, processed))
    data.toDF("collected", "processed")

  }

  def getYearOfAnalysis(df: DataFrame): Int = {
    df.select(
      col(ColumnEnumeration.YEAR)
    )
      .take(1)(0)
      .getInt(0)
  }




}
