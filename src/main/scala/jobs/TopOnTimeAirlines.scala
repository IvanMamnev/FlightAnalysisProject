package com.example
package jobs

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.language.postfixOps

class TopOnTimeAirlines(spark: SparkSession, jobConfig: JobConfig) extends FlightAnalizerJob(spark, jobConfig) {

  override val nameOfJob: String = "Top On Time Airlines"
  private val pathOfAirlineFlightsCounting: String = s"src/main/resources/airline_flights_counting/$yearOfAnalysis"
  private val pathOfHistoricalData: String = s"$pathOfAirlineFlightsCounting/historical_data"
  private val pathOfTopAirlines: String = s"$pathOfAirlineFlightsCounting/topAirlines"
  private val pathOfTempData: String = s"src/main/resources/temp/$yearOfAnalysis/airline_flights_counting"
  private val airlineFlightsCounting: DataFrame => DataFrame = ag.airlineFlightsCounting("FLIGHTS_WITHOUT_DELAYS")
  private val top10Airlines: DataFrame => DataFrame = f.filterTop10("FLIGHTS_WITHOUT_DELAYS", "desc")

  private val airlineFlightsDF: DataFrame = flightsDF
    .transform(f.filterOnTimeDeparture)
    .transform(f.filterOnTimeArrival)
    .transform(airlineFlightsCounting)

  private val checkDirectory: Boolean = historicalData.checkDirectory(pathOfAirlineFlightsCounting)

  private val aggregatedDataForTopAirlinesDF: DataFrame = {
    if(checkDirectory){
      val rawDataDF: DataFrame = historicalData.initWithHistoricalData(
        readerParquet,
        Seq(ColumnEnumeration.AIRLINE),
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

    writerParquet.writeResult(pathOfTopAirlines)(topAirlinesDF)

    val checkHistoricalData: Boolean = historicalData.checkDirectory(pathOfHistoricalData)

    writerParquet
      .writeHistoricalData(pathOfHistoricalData, pathOfTempData, checkHistoricalData, readerParquet)(aggregatedDataForTopAirlinesDF)


  }

}

