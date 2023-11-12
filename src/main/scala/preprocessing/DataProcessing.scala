package com.example
package preprocessing

import exeption_handing.DfValidator

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{coalesce, col, lit}

class DataProcessing extends DfValidator {

  def fillGaps(columns: Seq[String])(df: DataFrame): DataFrame = {
    validateColumnPresence(columns)(df)
    columns match {
      case Seq(historicalName, historicalValue, name, value) =>
        df.select(
          coalesce(col(historicalName), col(name)).as(name),
          coalesce(col(value), lit(0)).as(value),
          coalesce(col(historicalValue), lit(0)).as(historicalValue)
        )
      case Seq(historicalFirstName,historicalSecondName,historicalValue,
      firstName, secondName, value) =>
        df.select(
          coalesce(col(historicalFirstName), col(firstName)).as(firstName),
          coalesce(col(historicalSecondName), col(secondName)).as(secondName),
          coalesce(col(value), lit(0)).as(value),
          coalesce(col(historicalValue), lit(0)).as(historicalValue)
        )
      case Seq(historicalName, historicalValueOfFlights, historicalValueOfTime,
      historicalValueOfTotalTime, historicalValueOfPercent,
      name, valueOfFlights, valueOfTime, valueOfTotalTime, valueOfPercent) =>
        df.select(
          coalesce(col(historicalName), col(name)).as(name),
          coalesce(col(historicalValueOfFlights), lit(0)).as(historicalValueOfFlights),
          coalesce(col(valueOfFlights), lit(0)).as(valueOfFlights),
          coalesce(col(historicalValueOfTime), lit(0)).as(historicalValueOfTime),
          coalesce(col(valueOfTime), lit(0)).as(valueOfTime),
          coalesce(col(historicalValueOfTotalTime), lit(0)).as(historicalValueOfTotalTime),
          coalesce(col(valueOfTotalTime), lit(0)).as(valueOfTotalTime)
        )
    }
  }

  def combineData(columns: Seq[String])(df: DataFrame): DataFrame = {
    validateColumnPresence(columns)(df)
    columns match {
      case Seq(name, value, historicalValue) =>
        df.withColumn(
          value,
          col(value).plus(col(historicalValue)).as(value)
        )
        .select(
          col(name),
          col(value)
        )
      case Seq(firstName, secondName, value, historicalValue) =>
        df.withColumn(
          value,
          col(value).plus(col(historicalValue)).as(value)
        )
        .select(
          col(firstName),
          col(secondName),
          col(value)
        )
      case Seq(name, historicalValueOfFlights, valueOfFlights,
      historicalValueOfTime, valueOfTime,
      historicalValueOfTotalTime, valueOfTotalTime) =>
        df.withColumns(
          Map(
            valueOfFlights -> col(valueOfFlights).plus(col(historicalValueOfFlights)),
            valueOfTime -> col(valueOfTime).plus(col(historicalValueOfTime)),
            valueOfTotalTime -> col(valueOfTotalTime).plus(col(historicalValueOfTotalTime)),
          )
        )
        .withColumn("PERCENT", col(valueOfTime)/col(valueOfTotalTime)*lit(100))
        .select(
          col(name),
          col(valueOfFlights),
          col(valueOfTime),
          col(valueOfTotalTime),
          col("PERCENT")
        )
    }
  }





}
