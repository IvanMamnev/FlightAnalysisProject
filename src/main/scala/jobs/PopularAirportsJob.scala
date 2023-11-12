package com.example
package jobs

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.language.postfixOps

class PopularAirportsJob(spark: SparkSession, jobConfig: JobConfig) extends FlightAnalizerJob(spark, jobConfig) {

  override val nameOfJob: String = "Popular Airports Job"
  private val pathOfAirportFlightsCounting: String = s"src/main/resources/airport_flights_counting/$yearOfAnalysis"
  private val pathOfHistoricalData: String = s"$pathOfAirportFlightsCounting/historical_data"
  private val pathOfTopAirports: String = s"$pathOfAirportFlightsCounting/topAirports"
  private val pathOfTempData: String = s"src/main/resources/temp/$yearOfAnalysis/airport_flights_counting"
  private val checkDirectory: Boolean = historicalData.checkDirectory(pathOfAirportFlightsCounting)
  private val airportFlightsCounting: DataFrame => DataFrame = ag.airportFlightsCounting("COUNT_FLIGHTS")
  private val top10Airports: DataFrame => DataFrame = f.filterTop10("COUNT_FLIGHTS", "desc")

  private val airportFlightsDF: DataFrame = flightsDF
    .transform(f.filterNotCancelled)
    .transform(airportFlightsCounting)

  private val aggregatedDataForTopAirportsDF: DataFrame = {
    if(checkDirectory){
      val rawDataDF: DataFrame = historicalData.initWithHistoricalData(
        readerParquet,
        Seq(ColumnEnumeration.ORIGIN_AIRPORT),
        pathOfHistoricalData
        )(airportFlightsDF)
      val dataWithoutNullDF: DataFrame = dataProcessing.fillGaps(
        rawDataDF.columns
      )(rawDataDF)

      val resultDF = dataProcessing
        .combineData(
        dataWithoutNullDF.columns
      )(dataWithoutNullDF)

      resultDF

    } else {
      airportFlightsDF
    }
  }

  private val topAirportsDF: DataFrame = aggregatedDataForTopAirportsDF
    .transform(top10Airports)

  override def run(): Unit = {

    writerParquet.writeResult(pathOfTopAirports)(topAirportsDF)

    val checkHistoricalData: Boolean = historicalData.checkDirectory(pathOfHistoricalData)

    writerParquet.writeHistoricalData(pathOfHistoricalData, pathOfTempData, checkHistoricalData, readerParquet)(aggregatedDataForTopAirportsDF)


  }

}
