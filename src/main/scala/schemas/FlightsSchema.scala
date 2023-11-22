package com.example
package schemas

import constants.DfColumn

import org.apache.spark.sql.types._

object FlightsSchema extends Schema {

  override def getSchema(): StructType = {

    StructType(Seq(
      StructField(DfColumn.YEAR, IntegerType),
      StructField(DfColumn.MONTH, IntegerType),
      StructField(DfColumn.DAY, IntegerType),
      StructField(DfColumn.DAY_OF_WEEK, IntegerType),
      StructField(DfColumn.AIRLINE, StringType),
      StructField(DfColumn.FLIGHT_NUMBER,StringType),
      StructField(DfColumn.TAIL_NUMBER, StringType),
      StructField(DfColumn.ORIGIN_AIRPORT, StringType),
      StructField(DfColumn.DESTINATION_AIRPORT, StringType),
      StructField(DfColumn.SCHEDULED_DEPARTURE, StringType),
      StructField(DfColumn.DEPARTURE_TIME, StringType),
      StructField(DfColumn.DEPARTURE_DELAY, IntegerType),
      StructField(DfColumn.TAXI_OUT, IntegerType),
      StructField(DfColumn.WHEELS_OFF, StringType),
      StructField(DfColumn.SCHEDULED_TIME, IntegerType),
      StructField(DfColumn.ELAPSED_TIME, IntegerType),
      StructField(DfColumn.AIR_TIME, IntegerType),
      StructField(DfColumn.DISTANCE, IntegerType),
      StructField(DfColumn.WHEELS_ON, StringType),
      StructField(DfColumn.TAXI_IN, IntegerType),
      StructField(DfColumn.SCHEDULED_ARRIVAL, StringType),
      StructField(DfColumn.ARRIVAL_TIME, StringType),
      StructField(DfColumn.ARRIVAL_DELAY, IntegerType),
      StructField(DfColumn.DIVERTED, ShortType),
      StructField(DfColumn.CANCELLED, ShortType),
      StructField(DfColumn.CANCELLATION_REASON, StringType),
      StructField(DfColumn.AIR_SYSTEM_DELAY, IntegerType),
      StructField(DfColumn.SECURITY_DELAY, IntegerType),
      StructField(DfColumn.AIRLINE_DELAY, IntegerType),
      StructField(DfColumn.LATE_AIRCRAFT_DELAY, IntegerType),
      StructField(DfColumn.WEATHER_DELAY, IntegerType)
    ))
  }

}
