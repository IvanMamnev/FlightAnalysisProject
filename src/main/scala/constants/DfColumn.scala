package com.example
package constants

import scala.language.implicitConversions

object DfColumn extends  DfColumn {

  implicit def columnToString(col: DfColumn.Value): String = col.toString


}

 trait DfColumn extends Enumeration {

  val IATA_CODE,
  AIRLINE,
  AIRPORT,
  CITY,
  STATE,
  COUNTRY,
  LATITUDE,
  LONGITUDE,
  YEAR,
  MONTH,
  DAY,
  DAY_OF_WEEK,
  FLIGHT_NUMBER,
  TAIL_NUMBER,
  ORIGIN_AIRPORT,
  DESTINATION_AIRPORT,
  SCHEDULED_DEPARTURE,
  DEPARTURE_TIME,
  DEPARTURE_DELAY,
  TAXI_OUT,
  WHEELS_OFF,
  SCHEDULED_TIME,
  ELAPSED_TIME,
  AIR_TIME,
  DISTANCE,
  WHEELS_ON,
  TAXI_IN,
  SCHEDULED_ARRIVAL,
  ARRIVAL_TIME,
  ARRIVAL_DELAY,
  DIVERTED,
  CANCELLED,
  CANCELLATION_REASON,
  AIR_SYSTEM_DELAY,
  SECURITY_DELAY,
  AIRLINE_DELAY,
  LATE_AIRCRAFT_DELAY,
  WEATHER_DELAY  = Value

 }
