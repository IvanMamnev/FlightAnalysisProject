package com.example
package schemas

import org.apache.spark.sql.types._

object FlightsSchema extends Schema {

  override def getSchema(): StructType = {

    StructType(Seq(
      StructField(ColumnEnumeration.YEAR, IntegerType),
      StructField(ColumnEnumeration.MONTH, IntegerType),
      StructField(ColumnEnumeration.DAY, IntegerType),
      StructField(ColumnEnumeration.DAY_OF_WEEK, IntegerType),
      StructField(ColumnEnumeration.AIRLINE, StringType),
      StructField(ColumnEnumeration.FLIGHT_NUMBER,StringType),
      StructField(ColumnEnumeration.TAIL_NUMBER, StringType),
      StructField(ColumnEnumeration.ORIGIN_AIRPORT, StringType),
      StructField(ColumnEnumeration.DESTINATION_AIRPORT, StringType),
      StructField(ColumnEnumeration.SCHEDULED_DEPARTURE, StringType),
      StructField(ColumnEnumeration.DEPARTURE_TIME, StringType),
      StructField(ColumnEnumeration.DEPARTURE_DELAY, IntegerType),
      StructField(ColumnEnumeration.TAXI_OUT, IntegerType),
      StructField(ColumnEnumeration.WHEELS_OFF, StringType),
      StructField(ColumnEnumeration.SCHEDULED_TIME, IntegerType),
      StructField(ColumnEnumeration.ELAPSED_TIME, IntegerType),
      StructField(ColumnEnumeration.AIR_TIME, IntegerType),
      StructField(ColumnEnumeration.DISTANCE, IntegerType),
      StructField(ColumnEnumeration.WHEELS_ON, StringType),
      StructField(ColumnEnumeration.TAXI_IN, IntegerType),
      StructField(ColumnEnumeration.SCHEDULED_ARRIVAL, StringType),
      StructField(ColumnEnumeration.ARRIVAL_TIME, StringType),
      StructField(ColumnEnumeration.ARRIVAL_DELAY, IntegerType),
      StructField(ColumnEnumeration.DIVERTED, ShortType),
      StructField(ColumnEnumeration.CANCELLED, ShortType),
      StructField(ColumnEnumeration.CANCELLATION_REASON, StringType),
      StructField(ColumnEnumeration.AIR_SYSTEM_DELAY, IntegerType),
      StructField(ColumnEnumeration.SECURITY_DELAY, IntegerType),
      StructField(ColumnEnumeration.AIRLINE_DELAY, IntegerType),
      StructField(ColumnEnumeration.LATE_AIRCRAFT_DELAY, IntegerType),
      StructField(ColumnEnumeration.WEATHER_DELAY, IntegerType)
    ))
  }

}
