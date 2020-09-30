package com.maxus.sparksql.thread

import java.io.FileInputStream
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.maxus.sparksql.util.DBUtils.{getDBProMap, getDBProperties}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions


object CommonUtil {


  val spark = SparkInstance.getSparkSession()

  /**
    * spark读取db得到dataframe
    *
    * @param propertiesName
    * @param dbtable
    * @return
    */
  def readFromDB(propertiesName: String, dbtable: String, whereCondition: String = "1=1"): Option[DataFrame] = {
    val df = spark.read.format("jdbc")
      .options(getDBProMap(propertiesName, dbtable))
      .load()
      .where(whereCondition)
    Some(df)
  }

  /**
    * spark dataframe to db
    *
    * @param df
    * @param propertiesName
    * @param dbtable
    * @param saveMode
    * @param batchSize
    */
  def writeToDB(df: DataFrame, propertiesName: String, dbtable: String, saveMode: SaveMode, batchSize: String) = {
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

  /**
    * 解析dx的properties
    * @param propName
    * @return
    */
  def getDXPerties(propName: String): Option[Properties] = {
    val dir = s"/home/smcv/shell_sql/spark/conf/task_relation/${propName}"
    val st = new FileInputStream(dir)
    val properties = new Properties()
    try{
      properties.load(st)
      Some(properties)
    }catch{
      case e:Exception =>
        val mes = e.getMessage
        throw new Exception(s"$mes -- ${propName} not found")
    }
  }


  /**
    *  解析参数
    * @param pt_all
    * @return
    */
  def getParamsMap(pt_all: String):Map[String, String] = {
    var params = Map[String, String]()
    val cal = Calendar.getInstance()
    val ptF = new SimpleDateFormat("yyyyMMdd")
    val ddF = new SimpleDateFormat("yyyy-MM-dd")
    val ptF_sec = new SimpleDateFormat("yyyyMMddHHmmss")
    val ddF_sec = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    // pt相关字符串截取能得到的
    val pt_min = pt_all.length match {
      case 14 => pt_all.substring(0, 12)
      case _ => s"${pt_all}0000"
    }
    params += ("pt_min" -> pt_min)
    val pt_hour = pt_min.substring(0, 10)
    params += ("pt_hour" -> pt_hour)
    val pt = pt_hour.substring(0, 8)
    params += ("pt" -> pt)
    val pt_month = pt.substring(0,6)
    params += ("pt_month" -> pt_month)
    params += ("pt_mon01" -> s"${pt_month}01")


    // 设置基准时间
    cal.setTime(ptF.parse(pt))

    // data_date相关字符串截取能得到的
    val data_date = ddF.format(cal.getTime())
    params += ("data_date" -> data_date)
    params += ("data_date_mon01" -> s"${data_date.substring(0,7)}-01")
    params += ("data_date_year0101" -> s"${pt.substring(0,4)}-01-01")

    // n 天前
    var lastI: Int = 0
    for(i <- List(1, 2, 3, 4, 5, 6, 7, 8, 15, 30, 60, 90)){
      cal.add(Calendar.DATE, -(i - lastI))
      params += (s"pt_${i}_day_ago" -> ptF.format(cal.getTime()))
      params += (s"data_date_${i}_day_ago" -> ddF.format(cal.getTime()))
      lastI = i
    }

    // 重置pt基准
    cal.setTime(ptF.parse(pt))
    // 月末
    cal.set(Calendar.DAY_OF_MONTH, cal.getActualMaximum(Calendar.DAY_OF_MONTH))
    params += ("pt_mon31" -> ptF.format(cal.getTime()))
    params += ("data_date_mon31" -> ddF.format(cal.getTime()))
    // 月初
    cal.set(Calendar.DAY_OF_MONTH, 1)
    // 上月最后一天
    cal.add(Calendar.DATE, -1)
    val pt_lastmon31 = ptF.format(cal.getTime())
    val data_date_lastmon31 = ptF.format(cal.getTime())
    params += ("pt_lastmon31" -> pt_lastmon31)
    params += ("data_date_lastmon31" -> data_date_lastmon31)
    params += ("data_date_lastmonth" -> data_date_lastmon31.substring(0, 7))
    params += ("data_date_lastmon01" -> s"${data_date_lastmon31.substring(0, 7)}-01")
    params += ("pt_lastmon01" -> s"${pt_lastmon31.substring(0,6)}01")
    params += ("pt_lastmonth" -> pt_lastmon31.substring(0,6))


    // n 月前的月初
    cal.set(Calendar.DAY_OF_MONTH, 1)
    lastI = 1
    for(i <- List(2, 3, 5, 12, 24)){
      cal.add(Calendar.MONTH, -(i - lastI))
      params += (s"data_date01_${i}month_ago" -> ddF.format(cal.getTime()))
      lastI = i
    }

    if(pt_all.length == 14){
      // 重置pt基准
      cal.setTime(ptF_sec.parse(pt_all))
      cal.add(Calendar.MINUTE, 90)
      params += ("date_90min_later" -> ddF_sec.format(cal.getTime()))
    }

    // 重置pt基准
    cal.setTime(new java.util.Date())
    params += ("pt_sys" -> ptF.format(cal.getTime()))
    val datetime_sys = ddF_sec.format(cal.getTime())
    params += ("datetime_sys" -> datetime_sys)
    cal.add(Calendar.DATE, -1)
    params += ("pt_sys_1_day_ago" -> ptF.format(cal.getTime()))
    params += ("data_date_sys_mon01" -> s"${datetime_sys.substring(0,8)}01")


    //println(params)
    params

  }

}
