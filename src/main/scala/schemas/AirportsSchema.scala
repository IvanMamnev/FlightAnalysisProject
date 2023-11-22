package com.example
package schemas

import constants.DfColumn

import org.apache.spark.sql.types._

object AirportsSchema extends Schema {

  override def getSchema(): StructType = {

    StructType(Seq(
      StructField(DfColumn.IATA_CODE, StringType),
      StructField(DfColumn.AIRPORT, StringType),
      StructField(DfColumn.CITY, StringType),
      StructField(DfColumn.STATE, StringType),
      StructField(DfColumn.COUNTRY, StringType),
      StructField(DfColumn.LATITUDE, FloatType),
      StructField(DfColumn.LONGITUDE, FloatType)
    ))
  }

}
