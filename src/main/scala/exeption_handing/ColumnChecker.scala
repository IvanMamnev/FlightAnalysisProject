package com.example
package exeption_handing

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.DataFrame

private class ColumnChecker(df: DataFrame, requiredCols: Seq[String]) {

  private val columns = df.columns.toSeq
  private val missingCols = requiredCols.diff(columns)
  val log: Logger = LogManager.getRootLogger

  private def missingColumnsMessage(): String = {
    val missingColNames = missingCols.mkString(", ")
    val dfCols = columns.mkString(", ")
    s"The [$missingColNames] columns are not in the DF with columns: [$dfCols]"
  }

  def validateColumnPresence(): Unit = {
    if (missingCols.nonEmpty) {
      log.error("Column is missing")
      throw MissingColumnsException(missingColumnsMessage())
    }
  }

}
