package com.example

import readers.DataFrameParquetReader

import org.apache.spark.sql.DataFrame

import java.nio.file.{Files, Path, Paths}

class HistoricalData {

  def checkDirectory(directory: String): Boolean = {

    val path: Path = Paths.get(directory)
    Files.exists(path)
  }

  def initWithHistoricalData(
    reader: DataFrameParquetReader,
    joinColumnName: Seq[String],
    path: String)(df: DataFrame): DataFrame = {

    val dataDF = reader.readDataFromPath(path)

    if(dataDF.isEmpty) {
      df
    } else {
      val colNames = dataDF.columns
      val historicalColNames = colNames.map(f => f -> s"HISTORICAL_$f").toMap
      val historicalDataDF = dataDF.withColumnsRenamed(historicalColNames)
      val joinCondition = {
        joinColumnName match {
          case Seq(oneColumn) => df.col(oneColumn) === historicalDataDF.col(s"HISTORICAL_$oneColumn")
          case Seq(firstColumn, secondColumn) => df.col(firstColumn) === historicalDataDF.col(s"HISTORICAL_$firstColumn") &&
            df.col(secondColumn) === historicalDataDF.col(s"HISTORICAL_$secondColumn")
        }
      }
      val generalData: DataFrame = historicalDataDF.join(df, joinCondition, "outer")
          generalData
    }
  }
}
