package com.example
package writers

import org.apache.spark.sql.{DataFrame, SaveMode}


object DataframeCsvWriter {

  case class Config(path: String = "")
}

class DataframeCsvWriter(config: DataframeCsvWriter.Config) extends DataframeWriter {

  override def write(df: DataFrame): Unit = {

    df.write
      .format("csv")
      .save(config.path)

  }

  def writeMetaInfo(df: DataFrame): Unit = {

    df.write
      .format("csv")
      .mode(SaveMode.Append)
      .option("header", "true")
      .save("src/main/resources/meta_info.csv")

  }

}
