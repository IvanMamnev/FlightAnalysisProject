package com.example
package readers

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}


object DataframeCsvReader {

  case class Config(hasHeader: Boolean = true, separator: String = ",", path: String, schema: StructType = StructType(Seq()))
}

class DataframeCsvReader(spark: SparkSession, config: DataframeCsvReader.Config) extends DataFrameReader {

  override def read(): DataFrame = {

    if (config.schema.isEmpty) {
      spark.read
        .option("header", config.hasHeader)
        .option("sep", config.separator)
        .csv(config.path)
    } else {
      spark.read
        .option("header", config.hasHeader)
        .option("sep", config.separator)
        .schema(config.schema)
        .csv(config.path)
    }
  }
}
