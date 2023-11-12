package com.example
package writers

import readers.DataFrameParquetReader

import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.language.postfixOps
import scala.sys.process._


object DataframeParquetWriter {
  case class Config(path: String = "")
}
class DataframeParquetWriter(config: DataframeParquetWriter.Config) extends DataframeWriter {

  override def write(df: DataFrame): Unit = {
    df.write
      .save(config.path)
  }

  def writeResult(path:String)(df: DataFrame): Unit = {
    df.write
      .mode(SaveMode.Overwrite)
      .save(path)
  }

def writeHistoricalData(pathOfHistoricalData: String, pathOfTempData: String, checkPath: Boolean, reader: DataFrameParquetReader)
                       (df: DataFrame): Unit = {

  if (checkPath) {
    writeResult(pathOfTempData)(df)
    s"rm -r $pathOfHistoricalData" !
    val tempHistoricalData: DataFrame = reader.readDataFromPath(pathOfTempData)
    writeResult(pathOfHistoricalData)(tempHistoricalData)
    s"rm -r $pathOfTempData" !
  } else {
    writeResult(pathOfHistoricalData)(df)
  }


}

}
