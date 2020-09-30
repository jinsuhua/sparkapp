package com.maxus.sparksql

import org.apache.spark.sql.SparkSession

class SparkSessionBase {
  val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

}
