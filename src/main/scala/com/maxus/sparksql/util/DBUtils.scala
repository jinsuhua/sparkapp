package com.maxus.sparksql.util

import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object DBUtils {

  /**
    * 根据属性文件名称得到得到属性properties对象
    * @param propertiesName
    * @return
    */
  def getDBProperties(propertiesName: String): Option[Properties] = {
    CommonUtils.getProPerties("db", propertiesName)
  }

  /**
    * 得到map形式的属性
    *
    * @param propertiesName
    * @param dbtable
    * @return
    */
  def getDBProMap(propertiesName: String, dbtable: String): Map[String, String] ={
    val properties = getDBProperties(propertiesName).get
    var options = Map[String, String]()
    options += "url" -> properties.getProperty("url")
    options += "user" -> properties.getProperty("user")
    options += "password" -> properties.getProperty("password")
    options += "dbtable" -> dbtable
    options += "driver" -> properties.getProperty("driver")
    options
  }

  /**
    * 根据属性文件判断db类型
    *
    * @param dbProName
    * @return
    */
  def getDBType(dbProName: String): String ={
    val properties = getDBProperties(dbProName).get
    val driver = properties.getProperty("driver")
    var sql = ""
    if(driver.contains("mysql")){
      "mysql"
    }else if (driver.contains("oracle")){
      "oracle"
    }else{
      ""
    }
  }

  /**
    * spark读取db得到dataframe
    *
    * @param spark
    * @param propertiesName
    * @param dbtable
    * @return
    */
  def readFromDB(spark: SparkSession, propertiesName: String, dbtable: String, whereCondition: String = "1=1"): Option[DataFrame] = {
    val df = spark.read.format("jdbc")
      .options(getDBProMap(propertiesName, dbtable))
      .load()
      .where(whereCondition)
    Some(df)
  }

  /**
    * spark dataframe to db
    *
    * @param spark
    * @param df
    * @param propertiesName
    * @param dbtable
    * @param saveMode
    * @param batchSize
    */
  def writeToDB(spark: SparkSession, df: DataFrame, propertiesName: String, dbtable: String, saveMode: SaveMode, batchSize: String) = {
    val properties = getDBProperties(propertiesName).get
    var url = properties.getProperty("url")
    if(batchSize != null){
      url += "&rewriteBatchedStatements=true"
      properties.setProperty(JDBCOptions.JDBC_BATCH_INSERT_SIZE, batchSize)
    }
    df.write.mode(saveMode).jdbc(
      url,
      dbtable,
      properties)
  }




}
