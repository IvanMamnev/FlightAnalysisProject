package com.example
package preprocessing

import validator.DfValidator

import org.apache.spark.sql.functions.{coalesce, col, lit}
import org.apache.spark.sql.{Column, DataFrame}

class DataProcessing(validator: DfValidator) {

  def fillGaps(columns: Seq[String])(df: DataFrame): DataFrame = {
    validator.validateColumnPresence(columns)(df)
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
  private def plusTwoColumns(pairs: Seq[(String, String)])(df: DataFrame): DataFrame = {
    val newColumns: Map[String, Column] = pairs.map(pair =>
      pair._1 -> col(pair._1).plus(col(pair._2))
    ).toMap
    df.withColumns(
      newColumns
    )
  }
  def combineData(columns: Seq[String])(df: DataFrame): DataFrame = {
    validator.validateColumnPresence(columns)(df)
    columns match {

      case Seq(name, historicalValueOfFlights, valueOfFlights,
      historicalValueOfTime, valueOfTime,
      historicalValueOfTotalTime, valueOfTotalTime) =>

        val sumOfColumns: DataFrame => DataFrame = plusTwoColumns(
          Seq(
            (valueOfFlights, historicalValueOfFlights),
            (valueOfTime, historicalValueOfTime),
            (valueOfTotalTime, historicalValueOfTotalTime)
          )
        )
        df.transform(sumOfColumns)
        .withColumn("PERCENT", col(valueOfTime)/col(valueOfTotalTime)*lit(100))
        .select(
          col(name),
          col(valueOfFlights),
          col(valueOfTime),
          col(valueOfTotalTime),
          col("PERCENT")
        )

      case _ =>
        val columnForSum: Seq[String] = columns.takeRight(2)
        val pair: Seq[(String, String)] = Seq(
          (columnForSum.head, columnForSum(1))
        )
        val columnForSelect = columns
          .dropRight(1)
          .map(colName => col(colName))
        val res = plusTwoColumns(pair)(df)
          .select(columnForSelect: _*)
        res
    }
  }





}
