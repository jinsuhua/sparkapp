package com.maxus.sparksql.util

import scala.io.Source

object SQLUtils {
  def getSQLString(sqlName: String): Option[String] = {
    try{
      val stream = Thread.currentThread().getContextClassLoader.getResourceAsStream("sql/" + sqlName + ".sql")
      Some(Source.fromInputStream(stream).mkString)
    }catch{
      case e:Exception => None
    }
  }
}
