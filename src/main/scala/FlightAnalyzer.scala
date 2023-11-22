package com.example

import exeption_handing.SaveRunner
import jobs.{GeneralJob, JobConfig}
import readers.DataframeCsvReader
import schemas.FlightsSchema
import writers.{DataframeCsvWriter, DataframeParquetWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode

import scala.language.postfixOps
import scala.sys.process._

object FlightAnalyzer extends SessionWrapper {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    require(args.length == 1, "Specify the correct input data (the path for flights.csv)")

    val job = new GeneralJob(
      spark, JobConfig(
        DataframeCsvReader.Config(
          hasHeader = true,
          separator = ",",
          schema = FlightsSchema.getSchema()
        ),
        DataframeCsvWriter.Config(hasHeader = true, saveMode = SaveMode.Append),
        DataframeParquetWriter.Config(saveMode = SaveMode.Overwrite),
        args(0)
      )
    )

    SaveRunner.saveRun(job)

    s"mv ${args(0)} src/main/resources/analyzed" !

    spark.stop()

  }

}
