package com.example
package readers

import org.apache.spark.sql.{DataFrame, SparkSession}


class DataFrameParquetReader(spark: SparkSession) extends DataFrameReader {

  override def read(path: String): DataFrame = {
    spark.read
      .parquet(path)
  }

}
