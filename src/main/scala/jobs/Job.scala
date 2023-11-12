package com.example
package jobs

import org.apache.spark.sql.SparkSession

abstract class Job(spark: SparkSession, jobConfig: JobConfig) {

val nameOfJob: String

  def run(): Unit

}
