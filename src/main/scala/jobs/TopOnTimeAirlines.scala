package com.example
package jobs

import constants.{DfColumn, FilterCondition}
import writers.HistoricalParquetWriter

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.language.postfixOps

class TopOnTimeAirlines(spark: SparkSession, jobConfig: JobConfig) extends FlightAnalizerJob(spark, jobConfig) {

  override val nameOfJob: String = "Top On Time Airlines"
  private val pathOfAirlineFlightsCounting: String = s"src/main/resources/airline_flights_counting/$yearOfAnalysis"
  private val pathOfHistoricalData: String = s"$pathOfAirlineFlightsCounting/historical_data"
  private val pathOfTopAirlines: String = s"$pathOfAirlineFlightsCounting/topAirlines"
  private val pathOfTempData: String = s"src/main/resources/temp/$yearOfAnalysis/airline_flights_counting"
  private val airlineFlightsCounting: DataFrame => DataFrame = ag
    .aggregationByColumn(Seq(DfColumn.AIRLINE), "FLIGHTS_WITHOUT_DELAYS")
  private val top10Airlines: DataFrame => DataFrame = f.filterTop10("FLIGHTS_WITHOUT_DELAYS", "desc")
  private val filterOnTimeDeparture: DataFrame => DataFrame = f.filterWithCondition(FilterCondition.OnTimeDepartureCondition)
  private val filterOnTimeArrival: DataFrame => DataFrame = f.filterWithCondition(FilterCondition.OnTimeArrivalCondition)
  private val hasHistoricalData: Boolean = historicalData.checkDirectory(pathOfHistoricalData)
  private val historicalWriter: HistoricalParquetWriter = new HistoricalParquetWriter(pathOfTempData, hasHistoricalData, readerParquet, writerParquet)

  private val airlineFlightsDF: DataFrame = flightsDF
    .transform(filterOnTimeDeparture)
    .transform(filterOnTimeArrival)
    .transform(airlineFlightsCounting)

  private val hasDirectory: Boolean = historicalData.checkDirectory(pathOfAirlineFlightsCounting)

  private val aggregatedDataForTopAirlinesDF: DataFrame = {
    if(hasDirectory){
      val rawDataDF: DataFrame = historicalData.initWithHistoricalData(
        readerParquet,
        Seq(DfColumn.AIRLINE),
        pathOfHistoricalData
      )(airlineFlightsDF)
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
      airlineFlightsDF
    }
  }

  private val topAirlinesDF: DataFrame = aggregatedDataForTopAirlinesDF
    .transform(top10Airlines)

  override def run(): Unit = {

    writerParquet.write(pathOfTopAirlines)(topAirlinesDF)
    historicalWriter
      .write(pathOfHistoricalData)(aggregatedDataForTopAirlinesDF)

  }

}

