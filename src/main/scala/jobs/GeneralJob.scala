package com.example
package jobs

import exeption_handing.SaveRunner

import org.apache.spark.sql.SparkSession

class GeneralJob(spark: SparkSession, jobConfig: JobConfig) extends Job(spark, jobConfig) {

  override val nameOfJob: String = "General Job"
  private val delayReasonsRatioJob = new DelayReasonsRatioJob(spark, jobConfig)
  private val popularAirportsJob = new PopularAirportsJob(spark, jobConfig)
  private val topDaysOfWeakByFlights = new TopDaysOfWeakByFlights(spark, jobConfig)
  private val topDestinationAirportOnTime = new TopDestinationAirportOnTime(spark, jobConfig)
  private val topOnTimeAirlines = new TopOnTimeAirlines(spark, jobConfig)
  private val topOnTimeAirlinesAtAirport = new TopOnTimeAirlinesAtAirport(spark, jobConfig)
  private val metaINFOJob = new MetaINFOJob(spark, jobConfig)


  override def run(): Unit = {

    SaveRunner.saveRun(popularAirportsJob)
    SaveRunner.saveRun(topOnTimeAirlines)
    SaveRunner.saveRun(topOnTimeAirlinesAtAirport)
    SaveRunner.saveRun(topDestinationAirportOnTime)
    SaveRunner.saveRun(topDaysOfWeakByFlights)
    SaveRunner.saveRun(delayReasonsRatioJob)
    SaveRunner.saveRun(metaINFOJob)

  }
}
