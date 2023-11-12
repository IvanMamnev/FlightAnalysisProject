package com.example
package readers

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFrameParquetReader {
  case class Config(path: String = "")
}

class DataFrameParquetReader(spark: SparkSession, config: DataFrameParquetReader.Config) extends DataFrameReader {

  override def read(): DataFrame = {
    spark.read
      .parquet(config.path)
  }

  def readDataFromPath(path: String): DataFrame = {
    spark.read
      .parquet(path)
  }


}
