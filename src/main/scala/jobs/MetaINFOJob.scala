package com.example
package jobs

import metrics.DataAggregation
import readers.DataframeCsvReader
import transformers.DataConverter
import writers.DataframeCsvWriter

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.text.SimpleDateFormat
import java.util.Calendar

class MetaINFOJob(spark: SparkSession, jobConfig: JobConfig) extends Job(spark, jobConfig) {

  override val nameOfJob: String = "Meta INFO Job"
  private val readerCSV: DataframeCsvReader = new DataframeCsvReader(spark, jobConfig.readerCSVConfig)
  private val writerCSV: DataframeCsvWriter = new DataframeCsvWriter(jobConfig.writerCSVConfig)
  private val flightsDF: DataFrame = readerCSV.read()
  private val ag: DataAggregation = new DataAggregation
  private val cnvrt: DataConverter = new DataConverter



  private val dateDF: Dataset[Row] = flightsDF
    .transform(ag.groupingFlightByDate)

  private val beginningPeriodDF: Array[Row] = dateDF.take(1)
  private val endPeriodDF: Array[Row] = dateDF.tail(1)

  private val beginningPeriod: String = cnvrt.convertToDateForm(beginningPeriodDF)
  private val endPeriod: String = cnvrt.convertToDateForm(endPeriodDF)
  private val analysisPeriod: String = s"$beginningPeriod-$endPeriod"
  private val dateFormat = new SimpleDateFormat("dd.MM.yyyy")
  private val currentDate: String = dateFormat.format(Calendar.getInstance().getTime)
  private val metaInfoDF: DataFrame = cnvrt.createMetaInfo(analysisPeriod, currentDate)

  override def run(): Unit = {

    writerCSV.writeMetaInfo(metaInfoDF)

  }

}
