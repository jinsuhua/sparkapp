package com.maxus.sparksql.dao

import java.sql.{Connection, DriverManager, ResultSet}
import com.maxus.sparksql.util.DBUtils
import scala.collection.mutable.ArrayBuffer

abstract class AbstractDao (dbProName: String){
  /**
    * 判断是否存在某表
    * @param tableName
    * @return
    */
  def ifExistsTable(tableName: String): Boolean

  /**
    * db2hive 的字段类型转化
    * @param dbType
    * @return
    */
  def db2hiveTypeTran(dbType: String): String

  /**
    * db2hive 的字段类型转化
    * @param hiveType
    * @return
    */
  def hive2dbTypeTran(hiveType: String): String

  /**
    * 根据字段名称类型注释建表
    * @param cols
    */
  def createTableByCols(dbTableName: String, cols: List[(String, String, String)]): Unit

  /**
    * 得到查询hive表时候的字段连起来的字符串
    * @param tblName
    * @param ifDateInHive data_date是否在hive中有
    * @param mapping_cols db 与hive名字不一样时候的映射
    * @return
    */
  def getTableCols(tblName: String, ifDateInHive: Boolean, mapping_cols: String): String

  /**
    * 得到数据库连接
    * @return
    */
  def getConn(): Connection = {
    val properties = DBUtils.getDBProperties(dbProName).get
    properties.setProperty("charset","UTF-8")
    val driver = properties.getProperty("driver")
    val url = properties.getProperty("url")
    Class.forName(driver)
    val conn = DriverManager.getConnection(url, properties)
    conn
  }

  /**
    * 执行db语句，不返回值
    * @param sql
    */
  def exeSql(sql: String): Unit ={
    val conn = getConn()
    val sm = conn.prepareCall(sql)
    try {
      sm.execute()
      sm.close()
    }finally {
      if (sm != null) sm.close()
      if (conn != null) conn.close()
    }
  }

  /**
    * 执行有返回结果的sql
    * @param sql
    * @return
    */
  def getQueryResult(sql: String, f: ResultSet => List[Any]): Option[List[Any]] ={
    val conn = getConn()
    val sm = conn.prepareCall(sql)
    println(sql)
    try {
      val retSet = sm.executeQuery()
      val ret = f(retSet)
      if(retSet != null) retSet.close()
      Some(ret)
    }finally {
      if(sm != null) sm.close()
      if(conn !=null) conn.close()
    }
  }

  /**
    * 判断是否有结果
    *
    * @param resultSet
    * @return
    */
  def IfResultFun(resultSet: ResultSet): List[Boolean] = {
      List(resultSet.next())
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
  def twoResultFun(retSet: ResultSet): List[(String,String)] ={
    val buf = ArrayBuffer[(String,String)]()
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
  def threeResultFun(retSet: ResultSet): List[(String, String, String)] ={
    val buf = ArrayBuffer[(String, String, String)]()
    while (retSet.next()) {
      val col1 = retSet.getString(1).toLowerCase
      val col2 = retSet.getString(2).toLowerCase
      val col3 = retSet.getString(3)
      buf += Tuple3(col1, col2, col3)
    }
    buf.toList
  }


  /**
    * 处理非字符类型数据为空字符串导入不到db问题C
    * @param ret
    * @param ifDateInHive
    * @param mappingCols
    * @return
    */
  def combileCols(ret: List[Any], ifDateInHive: Boolean, mappingCols: String): String ={

    // 字段名不一样时候的映射
    var mappingColsMap:Map[String, String] = Map.empty
    if(mappingCols != null){
      mappingCols.split("\\|").map(col =>
        mappingColsMap += (col.split(":")(0)-> col.split(":")(1)))
    }

    val cols = ArrayBuffer[String]()
    for(single <- ret) {
      single match {
        case s: (String, String) @ unchecked =>
          val col_as = s._1.toLowerCase
          var col = col_as
          if(mappingColsMap.nonEmpty && mappingColsMap.contains(col_as)){
            col = mappingColsMap.get(col_as).get
          }
          val t = s._2.toLowerCase
          if (!col.equals("data_date")) {
            if (!(t.startsWith("varchar") || t.startsWith("text"))) {
              cols += s"if($col = '', null, $col) as $col_as"
            } else {
              cols += s"$col as $col_as"
            }
          }else {
            if(ifDateInHive){
              cols += s"$col"
            }
          }
        case _ =>
      }
    }

    cols.mkString(", ")
  }




}

