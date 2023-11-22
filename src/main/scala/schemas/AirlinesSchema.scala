package com.example
package schemas

import constants.DfColumn

import org.apache.spark.sql.types._

object AirlinesSchema extends Schema {

  override def getSchema(): StructType = {

    StructType(Seq(
      StructField(DfColumn.IATA_CODE, StringType),
      StructField(DfColumn.AIRLINE, StringType)
    ))

  }

}
