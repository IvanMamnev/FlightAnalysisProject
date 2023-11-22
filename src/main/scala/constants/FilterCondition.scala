package com.example
package constants

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

object FilterCondition {

  val NotCancelledCondition: Column = col(DfColumn.CANCELLED) === 0
  val OnTimeDepartureCondition: Column = col(DfColumn.DEPARTURE_DELAY) === 0
  val OnTimeArrivalCondition: Column = col(DfColumn.ARRIVAL_DELAY) === 0
  val WithDepartureDelayCondition: Column = col(DfColumn.DEPARTURE_DELAY) > 0
  val WithAirSystemDelayCondition: Column = col(DfColumn.AIR_SYSTEM_DELAY) > 0
  val WithSecurityDelayCondition: Column = col(DfColumn.SECURITY_DELAY) > 0
  val WithAirlineDelayCondition: Column = col(DfColumn.AIRLINE_DELAY) > 0
  val WithLateAircraftDelayCondition: Column = col(DfColumn.LATE_AIRCRAFT_DELAY) > 0
  val WithWeatherDelayCondition: Column = col(DfColumn.WEATHER_DELAY) > 0

}
