package com.maxus.sparksql.dao

case class MysqlDao(dbProName: String) extends AbstractDao(dbProName) {


  /**
    * mysql表是否存在
    *
    * @param tableName
    * @return
    */
  override def ifExistsTable(tableName: String): Boolean = {
    val sql = s"show tables like '$tableName'"
    val ret = getQueryResult(sql, IfResultFun).get
    ret.head match{
      case s: Boolean => s
    }
  }

  /**
    * msyql2hive 的字段类型转化
    * @param dbType
    * @return
    */
  override def db2hiveTypeTran(dbType: String): String ={

    val reg_char = """(var)?char\(\d+\)"""r
    val reg_text = """(tiny|long|medium)?text"""r
    val reg_int = """(tiny|small|medium)?int\(\d+\)"""r
    val reg_bigint ="""bigint\(\d\)"""r
    val reg_blob = """(tiny|long|medium)?blob"""r
    val reg_binary = """(var)?binary\(\d+\)"""r

    val hiveType = dbType match {
      case reg_char(s) => "string"
      case reg_text(s) => "string"
      case "json" => "string"
      case "date" => "string"
      case "time" => "string"
      case "datetime" => "string"
      case "timestamp" => "string"
      case reg_int(s) => "int"
      case reg_bigint() => "bigint"
      case reg_binary(s) => "binary"
      case reg_blob(s) => "binary"
      case _ => dbType
    }

    hiveType
  }

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
    * mysql create sql
    *
    * @param tblName
    * @return
    */
  def getMysqlCreateSql(tblName: String): String ={
    val sql = s"show create table $tblName"
    val ret = getQueryResult(sql, oneResultFun).get

    val s = ret.head match {
      case s: String => s
      case _ => ""
    }
    s
  }
  /**
    * 得到字段及类型，并组装字段字符串返回
    * 处理非字符类型数据为空字符串导入不到db问题
    * 需要区分oracle和mysql
    * hive2db
    *
    * @param tblName
    * @param ifDateInHive 是否data_date字段在hive里
    * @param mappingCols db 与hive名字不一样时候的映射
    * @return
    */
  override def getTableCols(tblName: String, ifDateInHive: Boolean, mappingCols: String): String = {
    val sql = s"desc $tblName"
    val ret = getQueryResult(sql, twoResultFun).get
    combileCols(ret, ifDateInHive, mappingCols)
  }


}
