package com.example
package validator

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.DataFrame

class ColumnValidator extends DfValidator {

  val log: Logger = LogManager.getRootLogger
  override protected def validate(requiredCols: Seq[String])(df: DataFrame): Unit = {
    val columns = df.columns.toSeq
    val missingCols = requiredCols.diff(columns)
    if (missingCols.nonEmpty) {
      log.error("Column is missing")
      throw MissingColumnsException(missingColumnsMessage(missingCols, columns))
    }
  }
  private def missingColumnsMessage(missingCols: Seq[String], columns: Seq[String]): String = {
    val missingColNames = missingCols.mkString(", ")
    val dfCols = columns.mkString(", ")
    s"The [$missingColNames] columns are not in the DF with columns: [$dfCols]"
  }

}
