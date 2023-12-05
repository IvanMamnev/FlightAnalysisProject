package com.example
package jobs

import constants.{DfColumn, FilterCondition}
import writers.HistoricalParquetWriter

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.language.postfixOps

class DelayReasonsRatioJob(spark: SparkSession, jobConfig: JobConfig) extends FlightAnalizerJob(spark, jobConfig) {

  import spark.implicits._

  override val nameOfJob: String = "Delay Reasons Ratio Job"
  private val pathOfDelayReasonsRatio: String = s"src/main/resources/delay_reasons_ratio/$yearOfAnalysis"
  private val pathOfHistoricalData: String = s"$pathOfDelayReasonsRatio/historical_data"
  private val pathOfDelaysSetOfIndicators: String = s"$pathOfDelayReasonsRatio/delay_reasons_set_of_indicator"
  private val pathOfTempData: String = s"src/main/resources/temp/$yearOfAnalysis/delay_reasons_ratio"
  private val hasDirectory: Boolean = historicalData.checkDirectory(pathOfDelayReasonsRatio)
  private val hasHistoricalData: Boolean = historicalData.checkDirectory(pathOfHistoricalData)
  private val historicalWriter: HistoricalParquetWriter = new HistoricalParquetWriter(pathOfTempData, hasHistoricalData, readerParquet, writerParquet)
  private val filterWithAirSystemDelay: DataFrame => DataFrame = f.filterWithCondition(FilterCondition.WithAirSystemDelayCondition)
  private val getTotalTimeAirSystemDelay: DataFrame => DataFrame = ag.totalTimeOfDelay(DfColumn.AIR_SYSTEM_DELAY)
  private val filterWithSecurityDelay: DataFrame => DataFrame = f.filterWithCondition(FilterCondition.WithSecurityDelayCondition)
  private val getTotalTimeSecurityDelay: DataFrame => DataFrame =  ag.totalTimeOfDelay(DfColumn.SECURITY_DELAY)
  private val filterWithAirlineDelay: DataFrame => DataFrame = f.filterWithCondition(FilterCondition.WithAirlineDelayCondition)
  private val getTotalTimeAirlineDelay: DataFrame => DataFrame = ag.totalTimeOfDelay(DfColumn.AIRLINE_DELAY)
  private val filterWithLateAircraftDelay: DataFrame => DataFrame = f.filterWithCondition(FilterCondition.WithLateAircraftDelayCondition)
  private val getTotalTimeLateAircraftDelay: DataFrame => DataFrame = ag.totalTimeOfDelay(DfColumn.LATE_AIRCRAFT_DELAY)
  private val filterWithWeatherDelay: DataFrame => DataFrame = f.filterWithCondition(FilterCondition.WithWeatherDelayCondition)
  private val getTotalTimeWeatherDelay: DataFrame => DataFrame = ag.totalTimeOfDelay(DfColumn.WEATHER_DELAY)
  private val filterWithDepartureDelay: DataFrame => DataFrame = f.filterWithCondition(FilterCondition.WithDepartureDelayCondition)
  private val getTotalTimeOfDelay: DataFrame => DataFrame = ag.totalTimeOfDelay(DfColumn.DEPARTURE_DELAY)

  private val airSystemDelayDF: DataFrame = flightsDF
    .transform(filterWithAirSystemDelay)
  private val flightsAirSystemDelay: Long = airSystemDelayDF.count()
  private val totalTimeAirSystemDelayDF: DataFrame = airSystemDelayDF
    .transform(getTotalTimeAirSystemDelay)
  private val totalTimeAirSystemDelay: Float = cnvrt.totalTimeToFloat(
    totalTimeAirSystemDelayDF
  )

  private val securityDelayDF: DataFrame = flightsDF
    .transform(filterWithSecurityDelay)
  private val flightsSecurityDelay: Long = securityDelayDF.count()
  private val totalTimeSecurityDelayDF: DataFrame = securityDelayDF
    .transform(getTotalTimeSecurityDelay)
  private val totalTimeSecurityDelay: Float = cnvrt.totalTimeToFloat(
    totalTimeSecurityDelayDF
  )

  private val airlineDelayDF: DataFrame = flightsDF
    .transform(filterWithAirlineDelay)
  private val flightsAirlineDelay: Long = airlineDelayDF.count()
  private val totalTimeAirlineDelayDF: DataFrame = airlineDelayDF
    .transform(getTotalTimeAirlineDelay)
  private val totalTimeAirlineDelay: Float = cnvrt.totalTimeToFloat(
    totalTimeAirlineDelayDF
  )

  private val lateTimeAircraftDelayDF: DataFrame = flightsDF
    .transform(filterWithLateAircraftDelay)
  private val flightsLateTimeAircraftDelay: Long = lateTimeAircraftDelayDF.count()
  private val totalTimeLateAircraftDelayDF: DataFrame = lateTimeAircraftDelayDF
    .transform(getTotalTimeLateAircraftDelay)
  private val totalTimeLateAircraftDelay: Float = cnvrt.totalTimeToFloat(
    totalTimeLateAircraftDelayDF
  )

  private val weatherDelayDF: DataFrame = flightsDF
    .transform(filterWithWeatherDelay)
  private val flightsWeatherDelayDF: Long = weatherDelayDF.count()
  private val totalTimeWeatherDelayDF: DataFrame = weatherDelayDF
    .transform(getTotalTimeWeatherDelay)

  private val totalTimeWeatherDelay: Float = cnvrt.totalTimeToFloat(
    totalTimeWeatherDelayDF
  )

  private val totalTimeDelayDF: DataFrame = flightsDF
    .transform(filterWithDepartureDelay)
    .transform(getTotalTimeOfDelay)
  private val totalTimeDelayReasons: Float = cnvrt.totalTimeToFloat(
    totalTimeDelayDF
  )

  private val percentAirSystemDelay: Float = totalTimeAirSystemDelay / totalTimeDelayReasons * 100
  private val percentSecurityDelay: Float = totalTimeSecurityDelay / totalTimeDelayReasons * 100
  private val percentAirlineDelay: Float = totalTimeAirlineDelay / totalTimeDelayReasons * 100
  private val percentLateAircraftDelay: Float = totalTimeLateAircraftDelay / totalTimeDelayReasons * 100
  private val percentTimeWeatherDelay: Float = totalTimeWeatherDelay / totalTimeDelayReasons * 100

  private val dataDelayReasons: Seq[(String, Long, Float, Float, Float)] = Seq(
    ("AIR_SYSTEM_DELAY", flightsAirSystemDelay, totalTimeAirSystemDelay, totalTimeDelayReasons, percentAirSystemDelay),
    ("SECURITY_DELAY", flightsSecurityDelay, totalTimeSecurityDelay, totalTimeDelayReasons, percentSecurityDelay),
    ("AIRLINE_DELAY", flightsAirlineDelay, totalTimeAirlineDelay, totalTimeDelayReasons, percentAirlineDelay),
    ("LATE_AIRCRAFT_DELAY", flightsLateTimeAircraftDelay, totalTimeLateAircraftDelay, totalTimeDelayReasons, percentLateAircraftDelay),
    ("WEATHER_DELAY", flightsWeatherDelayDF, totalTimeWeatherDelay, totalTimeDelayReasons, percentTimeWeatherDelay)
  )

  private val delayReasonsDF: DataFrame = dataDelayReasons.toDF(
    "TYPE_OF_DELAY", "FLIGHTS", "TIME", "TOTAL_TIME_OF_ALL_TYPES", "PERCENT"
  )

  private val resultDelayReasonsDF: DataFrame = {
    if (hasDirectory) {
      val rawDataDF: DataFrame = historicalData.initWithHistoricalData(
        readerParquet,
        Seq("TYPE_OF_DELAY"),
        pathOfHistoricalData
      )(delayReasonsDF)
      val dataWithoutNullDF: DataFrame =
        dataProcessing.fillGaps(
          rawDataDF.columns,
        )(rawDataDF)
      val resultDF: DataFrame = dataProcessing
        .combineData(
          dataWithoutNullDF.columns
        )(dataWithoutNullDF)

      resultDF

    } else {
      delayReasonsDF
    }
  }

  override def run(): Unit = {

    writerParquet.write(pathOfDelaysSetOfIndicators)(resultDelayReasonsDF)
    historicalWriter.write(pathOfHistoricalData)(resultDelayReasonsDF)

  }
}
