package com.example
package jobs

import readers.{DataFrameParquetReader, DataframeCsvReader}
import writers.{DataframeCsvWriter, DataframeParquetWriter}

case class JobConfig(
                    readerCSVConfig: DataframeCsvReader.Config,
                    readerParquetConfig: DataFrameParquetReader.Config,
                    writerCSVConfig: DataframeCsvWriter.Config,
                    writerParquetConfig: DataframeParquetWriter.Config
                    )
