package com.example
package jobs

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.language.postfixOps


class TopOnTimeAirlinesAtAirport(spark: SparkSession, jobConfig: JobConfig) extends FlightAnalizerJob(spark, jobConfig) {

  override val nameOfJob: String = "Top On Time Airlines At Airport"
  private val pathOfOnTimeAirlinesAtAirport: String = s"src/main/resources/airlines_at_airport/$yearOfAnalysis"
  private val pathOfHistoricalData: String = s"$pathOfOnTimeAirlinesAtAirport/historical_data"
  private val pathOfTopAirlines: String = s"$pathOfOnTimeAirlinesAtAirport/top_airlines_at_airport"
  private val pathOfTempData: String = s"src/main/resources/temp/$yearOfAnalysis/airlines_at_airport"
  private val airlineFlightsAtAirport: DataFrame => DataFrame = ag
    .parameterFlightsCountingAtAirports(
      ColumnEnumeration.AIRLINE,
      "COUNT_FLIGHTS")
  private val rankedAirlineFlightsAtAirport: DataFrame => DataFrame = ag
    .parameterFlightsRankingAtAirports(
      ColumnEnumeration.AIRLINE,
      "COUNT_FLIGHTS")
  private val top10AirlinesAtAirport: DataFrame => DataFrame = f.filterTop10(ColumnEnumeration.ORIGIN_AIRPORT, "asc")
  private val airlineFlightsAtAirportDF: DataFrame = flightsDF
    .transform(f.filterOnTimeDeparture)
    .transform(airlineFlightsAtAirport)

  private val checkDirectory: Boolean = historicalData.checkDirectory(pathOfOnTimeAirlinesAtAirport)

  private val aggregatedDataForTopAirlinesAtAirportDF: DataFrame = {
    if(checkDirectory){
      val rawDataDF: DataFrame = historicalData.initWithHistoricalData(
        readerParquet,
        Seq(ColumnEnumeration.ORIGIN_AIRPORT,
          ColumnEnumeration.AIRLINE),
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

    writerParquet.writeResult(pathOfTopAirlines)(topAirlinesAtAirportDF)

    val checkHistoricalData: Boolean = historicalData.checkDirectory(pathOfHistoricalData)

    writerParquet
      .writeHistoricalData(pathOfHistoricalData, pathOfTempData, checkHistoricalData, readerParquet)(aggregatedDataForTopAirlinesAtAirportDF)

  }

}
