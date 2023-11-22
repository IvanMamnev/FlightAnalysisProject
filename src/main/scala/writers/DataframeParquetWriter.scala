package com.example
package writers

import org.apache.spark.sql.{DataFrame, SaveMode}
import scala.language.postfixOps


object DataframeParquetWriter {
  case class Config(saveMode: SaveMode)
}
class DataframeParquetWriter(config: DataframeParquetWriter.Config) extends DataframeWriter {

  override def write(path: String)(df: DataFrame): Unit = {
    df.write
      .mode(config.saveMode)
      .save(path)
  }

}
