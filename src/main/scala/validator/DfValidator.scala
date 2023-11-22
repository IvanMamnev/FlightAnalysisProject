package com.example
package validator

import org.apache.spark.sql.DataFrame

trait DfValidator {

  def validateColumnPresence(requiredCols: Seq[String])(df: DataFrame): Unit = {
    validate(requiredCols)(df)
  }
  protected def validate (requiredCols: Seq[String])(df: DataFrame): Unit = {

  }

}
