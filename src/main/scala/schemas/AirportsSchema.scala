package com.example
package schemas

import org.apache.spark.sql.types._

object AirportsSchema extends Schema {

  override def getSchema(): StructType = {

    StructType(Seq(
      StructField(ColumnEnumeration.IATA_CODE, StringType),
      StructField(ColumnEnumeration.AIRPORT, StringType),
      StructField(ColumnEnumeration.CITY, StringType),
      StructField(ColumnEnumeration.STATE, StringType),
      StructField(ColumnEnumeration.COUNTRY, StringType),
      StructField(ColumnEnumeration.LATITUDE, FloatType),
      StructField(ColumnEnumeration.LONGITUDE, FloatType)
    ))
  }

}
