package com.maxus.sparksql.util

import java.io.{File, FileInputStream}
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import scala.io.Source

object CommonUtils {

  def getPtDaysAgo(pt: String, ptFormat: String, interval: Int, formatRet: String):String = {
    val dateFormat:SimpleDateFormat = new SimpleDateFormat(ptFormat)
    val dateFormat_rst:SimpleDateFormat = new SimpleDateFormat(formatRet)
    val cal:Calendar = Calendar.getInstance()
    cal.setTime(dateFormat.parse(pt))
    cal.add(Calendar.DATE, -interval)
    val dayago = dateFormat_rst.format(cal.getTime())
    dayago
  }

  def getProPerties(bus: String, propName: String): Option[Properties] = {
    //val st = Thread.currentThread().getContextClassLoader.getResourceAsStream("pro/"+ bus + "/" + propName + ".properties")
    val dir = s"/home/smcv/shell_sql/spark/conf/${bus}/${propName}.properties"
    //val dir = s"/home/smcv/shell_sql/tmp/zxx/spark/conf/${bus}/${propName}.properties"
    val st = new FileInputStream(dir)
    val properties = new Properties()
    try{
      properties.load(st)
      Some(properties)
    }catch{
      case e:Exception =>
        val mes = e.getMessage
        throw new Exception(s"$mes -- ${propName}.properties not found")
    }
  }


}
