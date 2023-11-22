package com.example
package readers

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}


object DataframeCsvReader {

  case class Config(hasHeader: Boolean, separator: String, schema: StructType)
}

class DataframeCsvReader(spark: SparkSession, config: DataframeCsvReader.Config) extends DataFrameReader {

  private val defaultOptions = Map(
    "header" -> config.hasHeader.toString,
    "sep" -> config.separator)

  private def readWithSchema(options: Map[String, String], path: String): DataFrame = {
    spark.read
      .options(options)
      .schema(config.schema)
      .csv(path)
  }

  private def readWithoutSchema(options: Map[String, String], path: String): DataFrame = {
    spark.read
      .options(options)
      .csv(path)
  }

  override def read(path: String): DataFrame = {
    if (config.schema.isEmpty) readWithoutSchema(defaultOptions, path)
    else readWithSchema(defaultOptions, path)
  }
}
