package com.example
package schemas

import org.apache.spark.sql.types._

object AirlinesSchema extends Schema {

  override def getSchema(): StructType = {

    StructType(Seq(
      StructField(ColumnEnumeration.IATA_CODE, StringType),
      StructField(ColumnEnumeration.AIRLINE, StringType)
    ))

  }

}
