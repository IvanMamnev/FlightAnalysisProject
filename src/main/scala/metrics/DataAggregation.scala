package com.example
package metrics

import constants.DfColumn
import validator.DfValidator

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, dense_rank, sum}

class DataAggregation(validator: DfValidator) {

  def airportFlightsCounting(aggregatedColumnName: String)(df: DataFrame): DataFrame = {
    validator.validateColumnPresence(Seq(DfColumn.ORIGIN_AIRPORT))(df)
    df.groupBy(col(DfColumn.ORIGIN_AIRPORT))
      .agg(count("*").as(aggregatedColumnName))
  }

  def airlineFlightsCounting(aggregatedColumnName: String)(df: DataFrame): DataFrame = {
    validator.validateColumnPresence(Seq(DfColumn.AIRLINE))(df)
    df.groupBy(col(DfColumn.AIRLINE))
      .agg(count("*").as(aggregatedColumnName))
  }

  def parameterFlightsCountingAtAirports(columnName: String, aggregatedColumnName: String)(df: DataFrame): DataFrame = {
    validator.validateColumnPresence(Seq(
      DfColumn.ORIGIN_AIRPORT,
      columnName))(df)

    df.groupBy(col(DfColumn.ORIGIN_AIRPORT),
        col(columnName)
      )
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

  def flightsByDayOfWeek(aggregatedColumnName: String)(df: DataFrame): DataFrame = {
    validator.validateColumnPresence(Seq(DfColumn.DAY_OF_WEEK))(df)
    df.groupBy(col(DfColumn.DAY_OF_WEEK))
      .agg(count("*").as(aggregatedColumnName))
  }

  def totalTimeDelayReasons(df: DataFrame): DataFrame = {
    validator.validateColumnPresence(Seq(DfColumn.DEPARTURE_DELAY))(df)
    df.agg(sum(col(DfColumn.DEPARTURE_DELAY)).as("TOTAL_DELAY"))
  }

  def totalTimeAirSystemDelay(df: DataFrame): DataFrame = {
    validator.validateColumnPresence(Seq(DfColumn.AIR_SYSTEM_DELAY))(df)
    df.agg(sum(col(DfColumn.AIR_SYSTEM_DELAY)).as("TOTAL_AIR_SYSTEM_DELAY"))
  }

  def totalTimeSecurityDelay(df: DataFrame): DataFrame = {
    validator.validateColumnPresence(Seq(DfColumn.SECURITY_DELAY))(df)
    df.agg(sum(col(DfColumn.SECURITY_DELAY)).as("TOTAL_SECURITY_DELAY"))
  }

  def totalTimeAirlineDelay(df: DataFrame): DataFrame = {
    validator.validateColumnPresence(Seq(DfColumn.AIRLINE_DELAY))(df)
    df.agg(sum(col(DfColumn.AIRLINE_DELAY)).as("TOTAL_AIRLINE_DELAY"))
  }

  def totalTimeLateAircraftDelay(df: DataFrame): DataFrame = {
    validator.validateColumnPresence(Seq(DfColumn.LATE_AIRCRAFT_DELAY))(df)
    df.agg(sum(col(DfColumn.LATE_AIRCRAFT_DELAY)))
  }

  def totalTimeWeatherDelay(df: DataFrame): DataFrame = {
    validator.validateColumnPresence(Seq(DfColumn.WEATHER_DELAY))(df)
    df.agg(sum(col(DfColumn.WEATHER_DELAY)))
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
