package com.maxus.sparksql

import com.maxus.sparksql.dao.{MysqlDao, OracleDao}
import com.maxus.sparksql.util.{CommonUtils, DBUtils, JavaUtils}
import com.sun.xml.internal.bind.v2.TODO
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions._
import scala.collection.mutable.ArrayBuffer



object Hive2DB extends SparkSessionBase {
  def main(args: Array[String]): Unit = {

    /*if (args.length < 4){
      throw new Exception("must at least 4 args (pt hivedb.hiveTableName dbProName dbTableName(oracle->user.dbtablename) [exportMode])")
    }
    println("传入的参数：")
    args.foreach(println _)

    // 月和日二种情况， 一般db库里月yyyy-MM， 日yyyy-MM-dd
    val pt = args(0)
    val data_date = pt.length match {
      case 6 => "%s-%s".format(pt.substring(0,4), pt.substring(4))
      case _ => CommonUtils.getPtDaysAgo(pt, "yyyyMMdd", 0, "yyyy-MM-dd")
    }

    val hiveTableName = args(1)
    val dbProName = args(2)
    val dbTableName = args(3)

    // 默认值
    var exportMode = "overwrite"
    var appendDelCondition = s"data_date = '$data_date'"

    //>4 说明设置了模式
    if(args.length > 4 && (args(4) == "overwrite" || args(4) == "append")){
      exportMode = args(4)
      if(args.length > 5){
        appendDelCondition = args(5)
      }
    }
    println("saveMode: " + exportMode)*/

    // 所有参数封装成Args类，方便扩展
    val options = new Hive2DBArgs("hive2db")
    // 参数解析
    options.parseArgs(args)

    //解析完参数后设置默认值
    val pt = options.pt
    val hiveTable = options.hive_table.trim()
    val dbPropName = options.db_prop_name.trim()
    val dbTable = options.db_table.trim()
    val complexTypeCols = options.complex_type_cols
    val batchSize = options.batch_size
    val mappingCols = options.mapping_cols

    val ptCondition = options.pt_condition match {
      case s: String => s
      case _ => s"pt='${pt}'"
    }
    val exportMode = options.export_mode match {
      case s: String => s
      case _ => "overwrite"
    }
    val ifAddDate = options.if_add_date match{
      case flg: String => flg.toBoolean
      case _ => false
    }
    val dataDate = options.data_date match {
      case s: String => s
      case _ => CommonUtils.getPtDaysAgo(pt, "yyyyMMdd", 0, "yyyy-MM-dd")
    }
    val dbDelCondition = options.db_del_condition match {
      case s: String => s
      case _ => s"data_date = '$dataDate'"
    }

    println("最终解析得到的参数：")
    println("pt : " + pt)
    println("hiveTable : " + hiveTable)
    println("dbPropName : " + dbPropName)
    println("dbTable : " + dbTable)
    println("ptCondition : " + ptCondition)
    println("exportMode : " + exportMode)
    println("dbDelCondition : " + dbDelCondition)
    println("ifAddDate : " + ifAddDate)
    println("dataDate : " + dataDate)
    println("complexTypeCols : " + complexTypeCols)
    println("batchSize : " + batchSize)
    println("mappingCols : " + mappingCols)



    spark.sparkContext.getConf.setAppName(s"hive2DB_$hiveTable")

    // 根据dbtype获取对应的dao对象
    val dao = DBUtils.getDBType(dbPropName) match {
      case "mysql" => MysqlDao(dbPropName)
      case "oracle" => OracleDao(dbPropName)
    }

    // 判断db是否有表
    val flag = dao.ifExistsTable(dbTable)
    var colString = ""
    var df = spark.emptyDataFrame

    //hive表中的字段
    val rows = spark.sql(s"desc $hiveTable").collect()
    val cols_arr:Array[String] = rows.filter(row => !row.getString(0).startsWith("#")).map(_.getString(0))

    if(flag){
      println("db里表存在，去db字段组装hivesql")
      // 查询hive表里是否有data_date字段
      val ifDateInHive = cols_arr.exists(_ == "data_date")

      // 查询db组装hivesql, 拿出hive数据到dataframe
      colString = dao.getTableCols(dbTable, ifDateInHive, mappingCols)

    }else{
      println("db里表不存在，直接取hive字段组装hivesql")
      // 去掉分区字段后的hive表里的col
      var cols_tmp = ArrayBuffer[String]()
      var colNames = ArrayBuffer[String]()
      for (field <- cols_arr){
        if(cols_tmp.contains(field)){
          // 去掉分区字段
          colNames = colNames.filter(!_.equals(field))
        }else{
          cols_tmp += field
          colNames += field
        }
      }
      colString = colNames.mkString(", ")

    }

    if(ifAddDate){
      colString = s"$colString, '$dataDate' as data_date"
    }

    // 准备好插入db的数据
    val hiveSql = s"select $colString from $hiveTable t where $ptCondition"
    println(s"hiveSql: $hiveSql")
    df = spark.sql(hiveSql)

    // 复杂类型的字段强转为string
    if(complexTypeCols != null){
      complexTypeCols.split(",").map(c => df = df.withColumn(c, col(c).cast(StringType)))
    }


    // 根据mode先操作db的表
    if(flag){
      val dbSql = exportMode match {
        case "overwrite" => s"truncate table $dbTable"
        case "append" => s"delete from $dbTable where $dbDelCondition"
      }
      println(s"dbSql: $dbSql")
      dao.exeSql(dbSql)
    }

    //write to db
    DBUtils.writeToDB(spark, df, dbPropName, dbTable, SaveMode.Append, batchSize)

    spark.stop()

  }




}

class Hive2DBArgs(busName: String) extends Args(busName) {
  var pt: String = null
  var pt_condition: String = null
  var hive_table: String= null
  var db_prop_name: String = null
  var db_table: String = null
  var export_mode: String = null
  var data_date: String = null
  var db_del_condition: String = null
  var if_add_date: String = null
  var complex_type_cols: String = null
  var batch_size: String = null
  var mapping_cols: String = null
}