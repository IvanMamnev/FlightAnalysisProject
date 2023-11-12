package com.example
package transformers

import exeption_handing.DfValidator

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class DataFiltering extends DfValidator {

  def filterNotCancelled(df: DataFrame): DataFrame = {

    validateColumnPresence(Seq(ColumnEnumeration.CANCELLED))(df)
    df.filter(col(ColumnEnumeration.CANCELLED) === 0)
  }

  def filterOnTimeDeparture(df: DataFrame): DataFrame = {
    validateColumnPresence(Seq(ColumnEnumeration.DEPARTURE_DELAY))(df)
    df.filter(col(ColumnEnumeration.DEPARTURE_DELAY) === 0)

  }

  def filterOnTimeArrival(df: DataFrame): DataFrame = {
    validateColumnPresence(Seq(ColumnEnumeration.ARRIVAL_DELAY))(df)
    df.filter(col(ColumnEnumeration.ARRIVAL_DELAY) === 0)
  }

  def filterTop10(colName: String, sortType: String)(df: DataFrame): DataFrame = {
    validateColumnPresence(Seq(colName))(df)

      require(Array("asc", "desc").contains(sortType),
      "Incorrect sort type")

    val columnsName = df.columns

    sortType match {
      case "asc" =>
        if (columnsName.contains("RANK")) {
          df.filter(col("RANK") <= 10)
            .orderBy(col(colName),
              col("RANK"))
        } else {
          df.orderBy(col(colName).desc)
            .limit(10)
            .orderBy(col(colName))
        }
      case "desc"=>
        if (columnsName.contains("RANK")) {
          df.filter(col("RANK") <= 10)
            .orderBy(col(colName),
              col("RANK").desc)
        } else {
          df.orderBy(col(colName).desc)
            .limit(10)
        }
    }
  }

  def filterWithDepartureDelay(df: DataFrame): DataFrame = {
    validateColumnPresence(Seq(ColumnEnumeration.DEPARTURE_DELAY))(df)
    df.filter(col(ColumnEnumeration.DEPARTURE_DELAY) > 0)
  }

  def filterWithAirSystemDelay(df: DataFrame): DataFrame = {
    validateColumnPresence(Seq(ColumnEnumeration.AIR_SYSTEM_DELAY))(df)
    df.filter(col(ColumnEnumeration.AIR_SYSTEM_DELAY) > 0)
  }

  def filterWithSecurityDelay(df: DataFrame): DataFrame = {
    validateColumnPresence(Seq(ColumnEnumeration.SECURITY_DELAY))(df)
    df.filter(col(ColumnEnumeration.SECURITY_DELAY) > 0)
  }

  def filterWithAirlineDelay(df: DataFrame): DataFrame = {
    validateColumnPresence(Seq(ColumnEnumeration.AIRLINE_DELAY))(df)
    df.filter(col(ColumnEnumeration.AIRLINE_DELAY) > 0)
  }

  def filterWithLateAircraftDelay(df: DataFrame): DataFrame = {
    validateColumnPresence(Seq(ColumnEnumeration.LATE_AIRCRAFT_DELAY))(df)
    df.filter(col(ColumnEnumeration.LATE_AIRCRAFT_DELAY) > 0)
  }

  def filterWithWeatherDelay(df: DataFrame): DataFrame = {
    validateColumnPresence(Seq(ColumnEnumeration.WEATHER_DELAY))(df)
    df.filter(col(ColumnEnumeration.WEATHER_DELAY) > 0)
  }


}
