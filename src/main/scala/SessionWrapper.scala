package com.example

import configs.Config

import org.apache.spark.sql.SparkSession

trait SessionWrapper {

  lazy val spark: SparkSession = SparkSession
    .builder()
    .appName("FlightAnalyzer")
    .config("spark.master", Config.get("master"))
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

}
