package com.example
package exeption_handing

import jobs.Job

import org.apache.log4j.LogManager

object SaveRunner {

  private val log = LogManager.getRootLogger
  def saveRun(job: Job): Unit = {

    try {
      job.run()
    } catch {
      case e: Exception => log.error(s"$e Exception in ${job.nameOfJob}")
        throw e
    }
  }
}
