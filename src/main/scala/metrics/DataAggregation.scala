package com.example
package metrics

import constants.DfColumn
import validator.DfValidator

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, dense_rank, sum}

class DataAggregation(validator: DfValidator) {

  def aggregationByColumn(nameOfColumns: Seq[String], aggregatedColumnName: String)(df: DataFrame): DataFrame = {
    validator.validateColumnPresence(nameOfColumns)(df)
    val columns: Seq[Column] = nameOfColumns.map(name => col(name))
    df.groupBy(columns: _*)
      .agg(count("*").as(aggregatedColumnName))
  }

  def parameterFlightsRankingAtAirports(columnName: String, basisOfRanking: String)(df: DataFrame): DataFrame = {
    validator.validateColumnPresence(Seq(
      DfColumn.ORIGIN_AIRPORT,
      columnName,
      basisOfRanking
    ))(df)

    val windowRank = Window.partitionBy(col(DfColumn.ORIGIN_AIRPORT))
      .orderBy(col(basisOfRanking))

    df.select(
      col(DfColumn.ORIGIN_AIRPORT),
      col(columnName),
      col(basisOfRanking),
      dense_rank().over(windowRank).as("RANK"))
  }

  def totalTimeOfDelay(colName: String)(df: DataFrame): DataFrame = {
    validator.validateColumnPresence(Seq(colName))(df)
    df.agg(sum(col(colName)))
  }

  def groupingFlightByDate(df: DataFrame): DataFrame = {
    validator.validateColumnPresence(Seq(
      DfColumn.YEAR,
      DfColumn.MONTH,
      DfColumn.DAY
    ))(df)
    df.groupBy(
      col(DfColumn.YEAR),
      col(DfColumn.MONTH),
      col(DfColumn.DAY)
    )
      .mean()
      .orderBy(
        col(DfColumn.YEAR),
        col(DfColumn.MONTH),
        col(DfColumn.DAY)
      )
  }


}
