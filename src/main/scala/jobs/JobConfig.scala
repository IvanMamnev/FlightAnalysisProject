package com.example
package jobs

import readers.DataframeCsvReader
import writers.{DataframeCsvWriter, DataframeParquetWriter}

case class JobConfig(
                    readerCSVConfig: DataframeCsvReader.Config,
                    writerCSVConfig: DataframeCsvWriter.Config,
                    writerParquetConfig: DataframeParquetWriter.Config,
                    pathOfInputData: String
                    )
