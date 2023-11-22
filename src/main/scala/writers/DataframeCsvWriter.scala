package com.example
package writers

import org.apache.spark.sql.{DataFrame, SaveMode}


object DataframeCsvWriter {
  case class Config(hasHeader: Boolean, saveMode: SaveMode)
}

class DataframeCsvWriter(config: DataframeCsvWriter.Config) extends DataframeWriter {

  override def write(path: String)(df: DataFrame): Unit = {

    df.write
      .format("csv")
      .mode(config.saveMode)
      .option("header", config.hasHeader)
      .save(path)
  }

}
