package com.example
package jobs

import constants.{DfColumn, FilterCondition}
import writers.HistoricalParquetWriter

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.language.postfixOps


class TopDaysOfWeakByFlights(spark: SparkSession, jobConfig: JobConfig) extends FlightAnalizerJob(spark, jobConfig) {

  override val nameOfJob: String = "Top Days Of Weak By Flights"
  private val pathOfDaysOfWeakAnalysis: String = s"src/main/resources/days_of_weak_by_flights/$yearOfAnalysis"
  private val pathOfHistoricalData: String = s"$pathOfDaysOfWeakAnalysis/historical_data"
  private val pathOfTopDaysOfWeak: String = s"$pathOfDaysOfWeakAnalysis/top_days_of_weak"
  private val pathOfTempData: String = s"src/main/resources/temp/$yearOfAnalysis/days_of_weak_by_flights"
  private val hasDirectory: Boolean = historicalData.checkDirectory(pathOfDaysOfWeakAnalysis)
  private val flightsByDaysOfWeek: DataFrame => DataFrame = ag.aggregationByColumn(Seq(DfColumn.DAY_OF_WEEK), "COUNT_FLIGHTS")
  private val topDaysOfWeek: DataFrame => DataFrame = f.filterTop10("COUNT_FLIGHTS", "desc")
  private val filterOnTimeDeparture: DataFrame => DataFrame = f.filterWithCondition(FilterCondition.OnTimeDepartureCondition)
  private val filterOnTimeArrival: DataFrame => DataFrame = f.filterWithCondition(FilterCondition.OnTimeArrivalCondition)
  private val hasHistoricalData: Boolean = historicalData.checkDirectory(pathOfHistoricalData)
  private val historicalWriter: HistoricalParquetWriter = new HistoricalParquetWriter(pathOfTempData, hasHistoricalData, readerParquet, writerParquet)

  private val daysOfWeekByFlightsDF: DataFrame = flightsDF
    .transform(filterOnTimeDeparture)
    .transform(filterOnTimeArrival)
    .transform(flightsByDaysOfWeek)

  private val aggregatedDataForTopDaysOfWeakDF: DataFrame = {
    if (hasDirectory) {
      val rawDataDF: DataFrame = historicalData.initWithHistoricalData(
        readerParquet,
        Seq(DfColumn.DAY_OF_WEEK),
        pathOfHistoricalData
      )(daysOfWeekByFlightsDF)
      val dataWithoutNullDF: DataFrame = dataProcessing.fillGaps(
        rawDataDF.columns
      )(rawDataDF)

      val resultDF = dataProcessing
        .combineData(
          dataWithoutNullDF.columns
        )(dataWithoutNullDF)

      resultDF

    } else {
      daysOfWeekByFlightsDF
    }
  }

  private val topDaysOfWeakByFlightsDF: DataFrame = aggregatedDataForTopDaysOfWeakDF
    .transform(topDaysOfWeek)

  override def run(): Unit = {

    writerParquet.write(pathOfTopDaysOfWeak)(topDaysOfWeakByFlightsDF)
    historicalWriter.write(pathOfHistoricalData)(aggregatedDataForTopDaysOfWeakDF)

  }

}
