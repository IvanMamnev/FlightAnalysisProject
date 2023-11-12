package com.example
package jobs

import metrics.DataAggregation
import preprocessing.DataProcessing
import readers.{DataFrameParquetReader, DataframeCsvReader}
import transformers.{DataConverter, DataFiltering}
import writers.DataframeParquetWriter

import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class FlightAnalizerJob(spark: SparkSession, jobConfig: JobConfig) extends Job(spark, jobConfig) {

  protected val readerCSV: DataframeCsvReader = new DataframeCsvReader(spark, jobConfig.readerCSVConfig)
  protected val readerParquet: DataFrameParquetReader = new DataFrameParquetReader(spark, jobConfig.readerParquetConfig)
  protected val writerParquet: DataframeParquetWriter = new DataframeParquetWriter(jobConfig.writerParquetConfig)
  protected val flightsDF: DataFrame = readerCSV.read()
  protected val f: DataFiltering = new DataFiltering
  protected val ag: DataAggregation = new DataAggregation
  protected val cnvrt: DataConverter = new DataConverter
  protected val dataProcessing: DataProcessing = new DataProcessing
  protected val yearOfAnalysis: Int = cnvrt.getYearOfAnalysis(flightsDF)
  protected val historicalData: HistoricalData = new HistoricalData

  override def run(): Unit = {

  }

}
