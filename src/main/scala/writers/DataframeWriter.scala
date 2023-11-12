package com.example
package writers

import org.apache.spark.sql.DataFrame

trait DataframeWriter {

  def write(df: DataFrame): Unit = {

  }

}
