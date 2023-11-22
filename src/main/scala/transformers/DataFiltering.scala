package com.example
package transformers

import validator.DfValidator

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame}

class DataFiltering(validator: DfValidator) {

  private def getColumnNameFromCondition(condition: Column): String = {

    condition.toString()
      .split(" ")(0)
      .tail
  }

  def filterWithCondition(condition: Column)(df: DataFrame): DataFrame = {

    val columnName: String = getColumnNameFromCondition(condition)
    validator.validateColumnPresence(Seq(columnName))(df)
    df.filter(condition)
  }


  def filterTop10(colName: String, sortType: String)(df: DataFrame): DataFrame = {
    validator.validateColumnPresence(Seq(colName))(df)

      require(Array("asc", "desc").contains(sortType),
      "Incorrect sort type")

    val columnsName = df.columns

    sortType match {
      case "asc" =>
        if (columnsName.contains("RANK")) {
          df.filter(col("RANK") <= 10)
            .orderBy(col(colName),
              col("RANK"))
        } else {
          df.orderBy(col(colName).desc)
            .limit(10)
            .orderBy(col(colName))
        }
      case "desc"=>
        if (columnsName.contains("RANK")) {
          df.filter(col("RANK") <= 10)
            .orderBy(col(colName),
              col("RANK").desc)
        } else {
          df.orderBy(col(colName).desc)
            .limit(10)
        }
    }
  }

}
