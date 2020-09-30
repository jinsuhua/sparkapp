package com.maxus.sparksql.thread

import org.apache.spark.sql.SparkSession

object SparkInstance {

  def getSparkSession():SparkSession = {
    SparkSession
      .builder()
      .appName("Relation-Service-v1.0")
      .config("spark.eventLog.enabled", "false")
      .config("spark.broadcast.blockSize", "32m")
      .config("spark.driver.memory", "10g")
      .config("spark.sql.codegen", "true")
      //.config("spark.sql.shuffle.partitions", "600")
      .enableHiveSupport()
      .getOrCreate()
  }

}