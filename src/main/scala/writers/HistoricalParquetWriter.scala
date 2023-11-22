package com.example
package writers
import readers.DataFrameParquetReader

import org.apache.spark.sql.DataFrame

import scala.language.postfixOps
import scala.sys.process._

class HistoricalParquetWriter(pathOfTempData: String,
                              checkPath: Boolean,
                              reader: DataFrameParquetReader,
                              writer: DataframeParquetWriter) extends DataframeWriter {

  override def write(pathOfHistoricalData: String)(df: DataFrame): Unit = {
    if(checkPath) writeWithTemp(pathOfHistoricalData)(df)
    else writeWithoutTemp(pathOfHistoricalData)(df)
  }

  private def writeWithTemp(pathOfHistoricalData: String)(df: DataFrame): Unit = {
    writer.write(pathOfTempData)(df)
    s"rm -r $pathOfHistoricalData" !
    val tempHistoricalData: DataFrame = reader.read(pathOfTempData)
    writer.write(pathOfHistoricalData)(tempHistoricalData)
    s"rm -r $pathOfTempData" !
  }

  private def writeWithoutTemp(pathOfHistoricalData: String)(df: DataFrame): Unit = {
    writer.write(pathOfHistoricalData)(df)
  }

}
