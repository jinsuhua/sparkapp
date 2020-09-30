/*
package com.maxus.sparksql.util

import java.io.FileInputStream
import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.mutable.ArrayBuffer

object DBUtils_bak {

  def getDBProperties(propertiesName: String): Option[Properties] = {
    //val st = Thread.currentThread().getContextClassLoader.getResourceAsStream("pro/" + propertiesName + ".properties")
    val dir = s"/home/smcv/shell_sql/spark/conf/${propertiesName}.properties"
    val st = new FileInputStream(dir)
    val properties = new Properties()
    try{
      properties.load(st)
      Some(properties)
    }catch{
      case e:Exception =>
        val mes = e.getMessage
        throw new Exception(s"$mes -- ${propertiesName}.properties not found")
    }
  }

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

  def readFromDB(spark: SparkSession, propertiesName: String, dbtable: String): Option[DataFrame] = {
    val df = spark.read.format("jdbc")
      .options(getDBProMap(propertiesName, dbtable))
      .load()
    Some(df)
  }

  def writeToDB(spark: SparkSession, df: DataFrame, propertiesName: String, dbtable: String, saveMode: SaveMode) = {
    val properties = getDBProperties(propertiesName).get
    df.write.mode(saveMode).jdbc(
          properties.getProperty("url"),
          dbtable,
          properties)
  }

  /**
    * 得到数据库连接
    * @param dbProName
    * @return
    */
  def getConn(dbProName: String): Connection = {
    val properties = getDBProperties(dbProName).get
    properties.setProperty("charset","UTF-8")
    val driver = properties.getProperty("driver")
    val url = properties.getProperty("url")
    Class.forName(driver)
    val conn = DriverManager.getConnection(url, properties)
    conn
  }

  /**
    * 执行db语句，不返回值
    * @param dbProName
    * @param sql
    */
  def exeSql(dbProName: String, sql: String): Unit ={
    val conn = getConn(dbProName)
    val sm = conn.prepareCall(sql)
    sm.execute()
    sm.close()
  }


  /**
    * 执行有返回结果的sql
    * @param dbProName
    * @param sql
    * @return
    */
  def getQueryResult(dbProName: String, sql: String, f: ResultSet => List[Any]): Option[List[Any]] ={
    val conn = getConn(dbProName)
    val sm = conn.prepareCall(sql)
    val retSet = sm.executeQuery()
    val ret = f(retSet)
    sm.close()
    Some(ret)
  }


  /**
    * 第一列 tablename
    * 第二列：
    * 1.mysql create table statement
    * 2.oracle table comment
    *
    * @param retSet
    * @return
    */
  def oneResultFun(retSet: ResultSet): List[String] ={
    var ret = ""
    if (retSet.next()) {
      ret = retSet.getString(2)
    }
    List(ret)
  }

  /**
    * mysql : col , type
    * @param retSet
    * @return
    */
  def twoResultFun(retSet: ResultSet): List[Tuple2[String,String]] ={
    val buf = ArrayBuffer[Tuple2[String,String]]()
    while (retSet.next()) {
      val col = retSet.getString(1).toLowerCase
      val t = retSet.getString(2).toLowerCase
      buf += Tuple2(col, t)
    }
    buf.toList
  }

  /**
    *  oracle ：cols, datatype, comments
    * @param retSet
    * @return
    */
  def threeResultFun(retSet: ResultSet): List[Tuple3[String, String, String]] ={
    val buf = ArrayBuffer[Tuple3[String, String, String]]()
    while (retSet.next()) {
      val col1 = retSet.getString(1).toLowerCase
      val col2 = retSet.getString(2).toLowerCase
      val col3 = retSet.getString(3)
      buf += Tuple3(col1, col2, col3)
    }
    buf.toList
  }

  /**
    * 得到字段及类型，并组装字段字符串返回
    * 处理非字符类型数据为空字符串导入不到db问题
    * 需要区分oracle和mysql
    * hive2db
    *
    * @param dbProName
    * @param tblName
    * @return
    */
  def getTableCols(dbProName: String, tblName: String): String ={
    val properties = getDBProperties(dbProName).get
    val driver = properties.getProperty("driver")
    var sql = ""
    if(driver.contains("mysql")){
      sql = s"desc $tblName"
    }else if (driver.contains("oracle")){
      val owner = tblName.split("\\.")(0)
      val tableName = tblName.split("\\.")(1)
      sql = s"select a.COLUMN_NAME, a.DATA_TYPE " +
        s"from all_tab_cols a " +
        s"left join all_col_comments b " +
        s"  on a.OWNER = b.OWNER and a.TABLE_NAME = b.TABLE_NAME and a.COLUMN_NAME = b.COLUMN_NAME " +
        s"where a.owner='$owner' and a.table_name='$tableName' " +
        s"and b.owner='$owner' and b.table_name='$tableName' " +
        s"order by a.COLUMN_ID"
    }

    val ret = getQueryResult(dbProName, sql, twoResultFun).get
    val cols = ArrayBuffer[String]()
    for(single <- ret) {
      if (single.isInstanceOf[Tuple2[String, String]]) {
        val single1 = single.asInstanceOf[Tuple2[String, String]]
        val col = single1._1
        val t = single1._2

        if (!(t.startsWith("varchar") || t.startsWith("text"))) {
          cols += s"if($col = '', null, $col) as $col"
        } else {
          cols += s"$col"
        }
      }
    }

    cols.mkString(", ")
  }

  /**
    * mysql create sql
    *
    * @param dbProName
    * @param tblName
    * @return
    */
  def getMysqlCreateSql(dbProName: String, tblName: String): String ={
    val sql = s"show create table $tblName"
    val ret = getQueryResult(dbProName, sql, oneResultFun).get

    val s = ret.head match {
      case s: String => s
      case _ => ""
    }
    s
  }


  /**
    * oracle table comment
    *
    * @param dbProName
    * @param tblName
    * @return
    */
  def getOracleTblComment(dbProName: String, tblName: String): String ={
    val owner = tblName.split("\\.")(0)
    val tableName = tblName.split("\\.")(1)
    val sql = s"select comments from all_tab_comments " +
      s"where a.owner='$owner' and a.table_name='$tableName'"
    val ret = getQueryResult(dbProName, sql, oneResultFun).get

    val s = ret.head match {
      case s: String => s
      case _ => ""
    }
    s
  }



  /**
    * 得到字段及类型及comment，并组装字段字符串返回
    * 处理非字符类型数据为空字符串导入不到db问题
    * 需要区分oracle和mysql
    * hive2db
    *
    * @param dbProName
    * @param tblName
    * @return
    */
  def getOracleCols(dbProName: String, tblName: String): List[Tuple3[String, String, String]] ={

    val owner = tblName.split("\\.")(0)
    val tableName = tblName.split("\\.")(1)
    val sql = s"select a.COLUMN_NAME, a.DATA_TYPE, b.COMMENTS " +
      s"from all_tab_cols a " +
      s"left join all_col_comments b " +
      s"  on a.OWNER = b.OWNER and a.TABLE_NAME = b.TABLE_NAME and a.COLUMN_NAME = b.COLUMN_NAME " +
      s"where a.owner='$owner' and a.table_name='$tableName' " +
      s"and b.owner='$owner' and b.table_name='$tableName' " +
      s"order by a.COLUMN_ID"


    val ret = getQueryResult(dbProName, sql, threeResultFun).get
    val cols = ArrayBuffer[Tuple3[String, String, String]]()
    for(single <- ret) {
      single match {
        case t3: Tuple3[String, String, String] =>
          val col = t3._1.toLowerCase
          val t = t3._2.toLowerCase
          val comment = t3._3
          cols += Tuple3(col, t, comment)
      }
    }
    cols.toList
  }


  /**
    * msyql2hive 的字段类型转化
    * @param mysqlType
    * @return
    */
  def mysql2hiveTypeTran(mysqlType: String): String ={

    val reg_char = """(var)?char\(\d+\)"""r
    val reg_text = """(tiny|long|medium)?text"""r
    val reg_int = """(tiny|small|medium)?int\(\d+\)"""r
    val reg_bigint ="""bigint\(\d\)"""r
    val reg_blob = """(tiny|long|medium)?blob"""r
    val reg_binary = """(var)?binary\(\d+\)"""r

    val hiveType = mysqlType match {
      case reg_char(s) => "string"
      case reg_text() => "string"
      case "json" => "string"
      case "date" => "string"
      case "time" => "string"
      case "datetime" => "string"
      case "timestamp" => "string"
      case reg_int(s) => "int"
      case reg_bigint() => "bigint"
      case reg_binary(s) => "binary"
      case reg_blob(s) => "binary"
      case _ => mysqlType
    }

    hiveType
  }

  /**
    * oracle2hive 的字段类型转化
    * @param oraType
    * @return
    */
  def oracle2hiveTypeTran(oraType: String): String ={

    null
  }




}
*/
