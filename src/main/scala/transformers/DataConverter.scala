package com.example
package transformers

import constants.DfColumn

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

  private def getValue(rawDate: Row)(position: Int): String = {
    rawDate
      .getInt(position)
      .toString
  }

  def convertToDateForm(date: Array[Row]): String = {

    val rawDate: Int => String = getValue(date(0))
    val year = rawDate(0)
    val month = rawDate(1)
    val day = rawDate(2)

    val dateForm = s"$year.${convertToDoubleDigit(month)}.${convertToDoubleDigit(day)}"
    dateForm

  }

  def createMetaInfo(collected: String, processed: String): DataFrame = {

    val data = Seq((collected, processed))
    data.toDF("collected", "processed")

  }

  def getYearOfAnalysis(df: DataFrame): Int = {
    df.select(
      col(DfColumn.YEAR)
    )
      .take(1)(0)
      .getInt(0)
  }

}
