package com.example
package jobs

import constants.{DfColumn, FilterCondition}
import writers.HistoricalParquetWriter

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.language.postfixOps

class TopDestinationAirportOnTime(spark: SparkSession, jobConfig: JobConfig) extends FlightAnalizerJob(spark, jobConfig) {

  override val nameOfJob: String = "Top Destination Airport On Time"
  private val pathOfDestinationAtAirport: String = s"src/main/resources/destination_at_airport/$yearOfAnalysis"
  private val pathOfHistoricalData: String = s"$pathOfDestinationAtAirport/historical_data"
  private val pathOfTopDestination: String = s"$pathOfDestinationAtAirport/top_destination_at_airport"
  private val pathOfTempData: String = s"src/main/resources/temp/$yearOfAnalysis/destination_at_airport"
  private val hasDirectory: Boolean = historicalData.checkDirectory(pathOfDestinationAtAirport)
  private val hasHistoricalData: Boolean = historicalData.checkDirectory(pathOfHistoricalData)
  private val historicalWriter: HistoricalParquetWriter = new HistoricalParquetWriter(pathOfTempData, hasHistoricalData, readerParquet, writerParquet)
  private val filterOnTimeDeparture: DataFrame => DataFrame = f.filterWithCondition(FilterCondition.OnTimeDepartureCondition)

  private val destinationAirportOnTime: DataFrame => DataFrame = ag
    .aggregationByColumn(Seq(
      DfColumn.ORIGIN_AIRPORT,
      DfColumn.DESTINATION_AIRPORT),
      "COUNT_FLIGHTS")
  private val rankedDestinationAirport: DataFrame => DataFrame = ag
    .parameterFlightsRankingAtAirports(
      DfColumn.DESTINATION_AIRPORT,
      "COUNT_FLIGHTS"
    )
  private val top10DestinationAirport: DataFrame => DataFrame = f.filterTop10(DfColumn.ORIGIN_AIRPORT, "asc")
  private val destinationAirportOnTimeDF: DataFrame = flightsDF
    .transform(filterOnTimeDeparture)
    .transform(destinationAirportOnTime)


  private val aggregatedDataForTopDestinationAtAirportDF: DataFrame = {
    if (hasDirectory) {
      val rawDataDF: DataFrame = historicalData.initWithHistoricalData(
        readerParquet,
        Seq(DfColumn.ORIGIN_AIRPORT,
          DfColumn.DESTINATION_AIRPORT),
        pathOfHistoricalData
      )(destinationAirportOnTimeDF)
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
      destinationAirportOnTimeDF
    }
  }

  private val topDestinationAirportOnTimeDF: DataFrame = aggregatedDataForTopDestinationAtAirportDF
    .transform(rankedDestinationAirport)
    .transform(top10DestinationAirport)

  override def run(): Unit = {

    writerParquet.write(pathOfTopDestination)(topDestinationAirportOnTimeDF)
    historicalWriter
      .write(pathOfHistoricalData)(aggregatedDataForTopDestinationAtAirportDF)

  }

}
