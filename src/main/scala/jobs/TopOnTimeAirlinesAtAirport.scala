package com.example
package jobs

import constants.{DfColumn, FilterCondition}
import writers.HistoricalParquetWriter

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.language.postfixOps


class TopOnTimeAirlinesAtAirport(spark: SparkSession, jobConfig: JobConfig) extends FlightAnalizerJob(spark, jobConfig) {

  override val nameOfJob: String = "Top On Time Airlines At Airport"
  private val pathOfOnTimeAirlinesAtAirport: String = s"src/main/resources/airlines_at_airport/$yearOfAnalysis"
  private val pathOfHistoricalData: String = s"$pathOfOnTimeAirlinesAtAirport/historical_data"
  private val pathOfTopAirlines: String = s"$pathOfOnTimeAirlinesAtAirport/top_airlines_at_airport"
  private val pathOfTempData: String = s"src/main/resources/temp/$yearOfAnalysis/airlines_at_airport"
  private val hasHistoricalData: Boolean = historicalData.checkDirectory(pathOfHistoricalData)
  private val historicalWriter: HistoricalParquetWriter = new HistoricalParquetWriter(pathOfTempData, hasHistoricalData, readerParquet, writerParquet)

  private val filterOnTimeDeparture: DataFrame => DataFrame = f.filterWithCondition(FilterCondition.OnTimeDepartureCondition)
  private val airlineFlightsAtAirport: DataFrame => DataFrame = ag
    .aggregationByColumn(Seq(
      DfColumn.ORIGIN_AIRPORT,
      DfColumn.AIRLINE),
      "COUNT_FLIGHTS")
  private val rankedAirlineFlightsAtAirport: DataFrame => DataFrame = ag
    .parameterFlightsRankingAtAirports(
      DfColumn.AIRLINE,
      "COUNT_FLIGHTS")
  private val top10AirlinesAtAirport: DataFrame => DataFrame = f.filterTop10(DfColumn.ORIGIN_AIRPORT, "asc")
  private val airlineFlightsAtAirportDF: DataFrame = flightsDF
    .transform(filterOnTimeDeparture)
    .transform(airlineFlightsAtAirport)

  private val hasDirectory: Boolean = historicalData.checkDirectory(pathOfOnTimeAirlinesAtAirport)

  private val aggregatedDataForTopAirlinesAtAirportDF: DataFrame = {
    if(hasDirectory){
      val rawDataDF: DataFrame = historicalData.initWithHistoricalData(
        readerParquet,
        Seq(DfColumn.ORIGIN_AIRPORT,
          DfColumn.AIRLINE),
        pathOfHistoricalData
      )(airlineFlightsAtAirportDF)
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
      airlineFlightsAtAirportDF
    }
  }

  private val topAirlinesAtAirportDF: DataFrame = aggregatedDataForTopAirlinesAtAirportDF
    .transform(rankedAirlineFlightsAtAirport)
    .transform(top10AirlinesAtAirport)

  override def run(): Unit = {

    writerParquet.write(pathOfTopAirlines)(topAirlinesAtAirportDF)
    historicalWriter
      .write(pathOfHistoricalData)(aggregatedDataForTopAirlinesAtAirportDF)

  }

}
