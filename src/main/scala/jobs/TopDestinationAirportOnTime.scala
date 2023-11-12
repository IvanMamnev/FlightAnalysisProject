package com.example
package jobs

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.language.postfixOps

class TopDestinationAirportOnTime(spark: SparkSession, jobConfig: JobConfig) extends FlightAnalizerJob(spark, jobConfig) {

  override val nameOfJob: String = "Top Destination Airport On Time"
  private val pathOfDestinationAtAirport: String = s"src/main/resources/destination_at_airport/$yearOfAnalysis"
  private val pathOfHistoricalData: String = s"$pathOfDestinationAtAirport/historical_data"
  private val pathOfTopDestination: String = s"$pathOfDestinationAtAirport/top_destination_at_airport"
  private val pathOfTempData: String = s"src/main/resources/temp/$yearOfAnalysis/destination_at_airport"
  private val destinationAirportOnTime: DataFrame => DataFrame = ag
    .parameterFlightsCountingAtAirports(
      ColumnEnumeration.DESTINATION_AIRPORT,
      "COUNT_FLIGHTS"
    )
  private val rankedDestinationAirport: DataFrame => DataFrame = ag
    .parameterFlightsRankingAtAirports(
      ColumnEnumeration.DESTINATION_AIRPORT,
      "COUNT_FLIGHTS"
    )
  private val top10DestinationAirport: DataFrame => DataFrame = f.filterTop10(ColumnEnumeration.ORIGIN_AIRPORT, "asc")
  private val destinationAirportOnTimeDF: DataFrame = flightsDF
    .transform(f.filterOnTimeDeparture)
    .transform(destinationAirportOnTime)

  private val checkDirectory: Boolean = historicalData.checkDirectory(pathOfDestinationAtAirport)

  private val aggregatedDataForTopDestinationAtAirportDF: DataFrame = {
    if (checkDirectory) {
      val rawDataDF: DataFrame = historicalData.initWithHistoricalData(
        readerParquet,
        Seq(ColumnEnumeration.ORIGIN_AIRPORT,
          ColumnEnumeration.DESTINATION_AIRPORT),
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

    writerParquet.writeResult(pathOfTopDestination)(topDestinationAirportOnTimeDF)

    val checkHistoricalData: Boolean = historicalData.checkDirectory(pathOfHistoricalData)

    writerParquet
      .writeHistoricalData(pathOfHistoricalData, pathOfTempData, checkHistoricalData, readerParquet)(aggregatedDataForTopDestinationAtAirportDF)


  }

}
