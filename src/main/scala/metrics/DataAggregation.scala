package com.example
package metrics

import exeption_handing.DfValidator

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, dense_rank, sum}

class DataAggregation extends DfValidator{

  def airportFlightsCounting(aggregatedColumnName: String)(df: DataFrame): DataFrame = {
    validateColumnPresence(Seq(ColumnEnumeration.ORIGIN_AIRPORT))(df)
    df.groupBy(col(ColumnEnumeration.ORIGIN_AIRPORT))
      .agg(count("*").as(aggregatedColumnName))
  }

  def airlineFlightsCounting(aggregatedColumnName: String)(df: DataFrame): DataFrame = {
    validateColumnPresence(Seq(ColumnEnumeration.AIRLINE))(df)
    df.groupBy(col(ColumnEnumeration.AIRLINE))
      .agg(count("*").as(aggregatedColumnName))
  }

  def parameterFlightsCountingAtAirports(columnName: String, aggregatedColumnName: String)(df: DataFrame): DataFrame = {
    validateColumnPresence(Seq(
      ColumnEnumeration.ORIGIN_AIRPORT,
      columnName))(df)

    df.groupBy(col(ColumnEnumeration.ORIGIN_AIRPORT),
        col(columnName)
      )
      .agg(count("*").as(aggregatedColumnName))
  }

  def parameterFlightsRankingAtAirports(columnName: String, basisOfRanking: String)(df: DataFrame): DataFrame = {
    validateColumnPresence(Seq(
      ColumnEnumeration.ORIGIN_AIRPORT,
      columnName,
      basisOfRanking
    ))(df)

    val windowRank = Window.partitionBy(col(ColumnEnumeration.ORIGIN_AIRPORT))
      .orderBy(col(basisOfRanking))

    df.select(
      col(ColumnEnumeration.ORIGIN_AIRPORT),
      col(columnName),
      col(basisOfRanking),
      dense_rank().over(windowRank).as("RANK"))

  }

  def flightsByDayOfWeek(aggregatedColumnName: String)(df: DataFrame): DataFrame = {
    validateColumnPresence(Seq(ColumnEnumeration.DAY_OF_WEEK))(df)
    df.groupBy(col(ColumnEnumeration.DAY_OF_WEEK))
      .agg(count("*").as(aggregatedColumnName))
  }

  def totalTimeDelayReasons(df: DataFrame): DataFrame = {
    validateColumnPresence(Seq(ColumnEnumeration.DEPARTURE_DELAY))(df)
    df.agg(sum(col(ColumnEnumeration.DEPARTURE_DELAY)).as("TOTAL_DELAY"))
  }

  def totalTimeAirSystemDelay(df: DataFrame): DataFrame = {
    validateColumnPresence(Seq(ColumnEnumeration.AIR_SYSTEM_DELAY))(df)
    df.agg(sum(col(ColumnEnumeration.AIR_SYSTEM_DELAY)).as("TOTAL_AIR_SYSTEM_DELAY"))
  }

  def totalTimeSecurityDelay(df: DataFrame): DataFrame = {
    validateColumnPresence(Seq(ColumnEnumeration.SECURITY_DELAY))(df)
    df.agg(sum(col(ColumnEnumeration.SECURITY_DELAY)).as("TOTAL_SECURITY_DELAY"))
  }

  def totalTimeAirlineDelay(df: DataFrame): DataFrame = {
    validateColumnPresence(Seq(ColumnEnumeration.AIRLINE_DELAY))(df)
    df.agg(sum(col(ColumnEnumeration.AIRLINE_DELAY)).as("TOTAL_AIRLINE_DELAY"))
  }

  def totalTimeLateAircraftDelay(df: DataFrame): DataFrame = {
    validateColumnPresence(Seq(ColumnEnumeration.LATE_AIRCRAFT_DELAY))(df)
    df.agg(sum(col(ColumnEnumeration.LATE_AIRCRAFT_DELAY)))
  }

  def totalTimeWeatherDelay(df: DataFrame): DataFrame = {
    validateColumnPresence(Seq(ColumnEnumeration.WEATHER_DELAY))(df)
    df.agg(sum(col(ColumnEnumeration.WEATHER_DELAY)))
  }

  def groupingFlightByDate(df: DataFrame): DataFrame = {
    validateColumnPresence(Seq(
      ColumnEnumeration.YEAR,
      ColumnEnumeration.MONTH,
      ColumnEnumeration.DAY
    ))(df)
    df.groupBy(
      col(ColumnEnumeration.YEAR),
      col(ColumnEnumeration.MONTH),
      col(ColumnEnumeration.DAY)
    )
      .mean()
      .orderBy(
        col(ColumnEnumeration.YEAR),
        col(ColumnEnumeration.MONTH),
        col(ColumnEnumeration.DAY)
      )
  }


}
