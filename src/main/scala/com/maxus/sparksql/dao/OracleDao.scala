package com.maxus.sparksql.dao

import scala.collection.mutable.ArrayBuffer

case class OracleDao(dbProName: String) extends AbstractDao(dbProName) {
  /**
    * 判断是否存在某表
    *
    * @param tblName
    * @return
    */
  override def ifExistsTable(tblName: String): Boolean = {
    val owner = tblName.split("\\.")(0).toUpperCase()
    val tableName = tblName.split("\\.")(1).toUpperCase()
    val sql = s"select * from all_all_tables a where a.owner='$owner' and a.table_name='$tableName'"
    val ret = getQueryResult(sql, IfResultFun).get
    ret.head match{
      case s: Boolean => s
    }
  }

  // TODO:
  /**
    * oracle 字段类型转化hive类型
    *
    * @param dbType
    * @return
    */
  override def db2hiveTypeTran(dbType: String): String = ???

  // TODO:
  /**
    * db2hive 的字段类型转化
    *
    * @param hiveType
    * @return
    */
  override def hive2dbTypeTran(hiveType: String): String = ???

  // TODO:
  /**
    * 根据字段名称类型注释建表
    *
    * @param cols
    */
  override def createTableByCols(dbTableName: String, cols: List[(String, String, String)]): Unit = ???


  /**
    * 得到字段及类型，并组装字段字符串返回
    * 处理非字符类型数据为空字符串导入不到db问题
    * 需要区分oracle和mysql
    * hive2db
    *
    * @param tblName
    * @param ifDateInHive hive里是否有data_date字段
    * @param mappingCols db 与hive名字不一样时候的映射
    * @return
    */
  override def getTableCols(tblName: String, ifDateInHive: Boolean, mappingCols: String): String ={

    val owner = tblName.split("\\.")(0).toUpperCase()
    val tableName = tblName.split("\\.")(1).toUpperCase()
    val sql = s"select a.COLUMN_NAME, a.DATA_TYPE " +
      s"from all_tab_cols a " +
      s"left join all_col_comments b " +
      s"  on a.OWNER = b.OWNER and a.TABLE_NAME = b.TABLE_NAME and a.COLUMN_NAME = b.COLUMN_NAME " +
      s"where a.owner='$owner' and a.table_name='$tableName' " +
      s"and b.owner='$owner' and b.table_name='$tableName' " +
      s"order by a.COLUMN_ID"
    val ret = getQueryResult(sql, twoResultFun).get
    combileCols(ret, ifDateInHive, mappingCols)
  }


  /**
    * 得到字段及类型及comment，并组装字段字符串返回
    * 处理非字符类型数据为空字符串导入不到db问题
    * 需要区分oracle和mysql
    * hive2db
    *
    * @param tblName
    * @return
    */
  def getOracleCols(tblName: String): List[Tuple3[String, String, String]] ={

    val owner = tblName.split("\\.")(0)
    val tableName = tblName.split("\\.")(1)
    val sql = s"select a.COLUMN_NAME, a.DATA_TYPE, b.COMMENTS " +
      s"from all_tab_cols a " +
      s"left join all_col_comments b " +
      s"  on a.OWNER = b.OWNER and a.TABLE_NAME = b.TABLE_NAME and a.COLUMN_NAME = b.COLUMN_NAME " +
      s"where a.owner='$owner' and a.table_name='$tableName' " +
      s"and b.owner='$owner' and b.table_name='$tableName' " +
      s"order by a.COLUMN_ID"


    val ret = getQueryResult(sql, threeResultFun).get
    val cols = ArrayBuffer[(String, String, String)]()
    for(single <- ret) {
      single match {
        case t3: (String, String, String) @unchecked =>
          val col = t3._1.toLowerCase
          val t = t3._2.toLowerCase
          val comment = t3._3
          cols += Tuple3(col, t, comment)
      }
    }
    cols.toList
  }

  /**
    * oracle table comment
    *
    * @param tblName
    * @return
    */
  def getOracleTblComment(tblName: String): String ={
    val owner = tblName.split("\\.")(0)
    val tableName = tblName.split("\\.")(1)
    val sql = s"select comments from all_tab_comments " +
      s"where a.owner='$owner' and a.table_name='$tableName'"
    val ret = getQueryResult(sql, oneResultFun).get

    val s = ret.head match {
      case s: String => s
      case _ => ""
    }
    s
  }

}
