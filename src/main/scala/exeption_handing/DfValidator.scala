package com.example
package exeption_handing

import org.apache.spark.sql.DataFrame

trait DfValidator {

  def validateColumnPresence(requiredCols: Seq[String])(df: DataFrame): Unit = {
    val checker = new ColumnChecker(df, requiredCols)
    checker.validateColumnPresence()
  }

}
