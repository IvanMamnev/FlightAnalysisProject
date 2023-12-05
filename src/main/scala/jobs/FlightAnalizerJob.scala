package com.example
package jobs

import metrics.DataAggregation
import preprocessing.{DataProcessing, HistoricalDataManager}
import readers.{DataFrameParquetReader, DataframeCsvReader}
import transformers.{DataConverter, DataFiltering}
import validator.{ColumnValidator, DfValidator}
import writers.DataframeParquetWriter

import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class FlightAnalizerJob(spark: SparkSession, jobConfig: JobConfig) extends Job(spark, jobConfig) {

  protected val readerCSV: DataframeCsvReader = new DataframeCsvReader(spark, jobConfig.readerCSVConfig)
  protected val readerParquet: DataFrameParquetReader = new DataFrameParquetReader(spark)
  protected val writerParquet: DataframeParquetWriter = new DataframeParquetWriter(jobConfig.writerParquetConfig)
  protected val validator: DfValidator = new ColumnValidator
  protected val flightsDF: DataFrame = readerCSV.read(jobConfig.pathOfInputData)
  protected val f: DataFiltering = new DataFiltering(validator)
  protected val ag: DataAggregation = new DataAggregation(validator)
  protected val cnvrt: DataConverter = new DataConverter
  protected val dataProcessing: DataProcessing = new DataProcessing(validator)
  protected val yearOfAnalysis: Int = cnvrt.getYearOfAnalysis(flightsDF)
  protected val historicalData: HistoricalDataManager = new HistoricalDataManager

  override def run(): Unit = {

  }

}
