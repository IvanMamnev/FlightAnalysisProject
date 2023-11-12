package com.example
package schemas

import org.apache.spark.sql.types.StructType

trait Schema {

  def getSchema: StructType

}
