package com.maxus.sparksql

import com.maxus.sparksql.dao.{AbstractDao, MysqlDao, OracleDao}
import com.maxus.sparksql.util.{CommonUtils, DBUtils}

import scala.collection.mutable.ArrayBuffer


/**
  * 从数据库读取表到hive的his表
  */
object DB2His extends SparkSessionBase {
  def main(args: Array[String]): Unit = {

    /*if (args.length < 4){
      throw new Exception("must at least 4 args (pt dbProName dbTableName(oracle->user.dbtablename) hivedb.hiveTableName [importMode])")
    }
    println("传入的参数：")
    args.foreach(println _)

    //parse args (默认全量导入)
    val pt = args(0)
    val dbProName = args(1)
    val dbTableName = args(2)
    val hiveTableName = args(3)

    var importMode = "all"
    var incCondition = ""
    //>4 说明设置了增量还是全量模式
    if(args.length > 4 && (args(4) == "all" || args(4) == "inc")){
      importMode = args(4)
      if(args.length > 5){
        incCondition = args(5)
      }
    }*/


    // 所有参数封装成Args类，方便扩展
    val options = new DB2HiveArgs("db2his")
    // 参数解析
    options.parseArgs(args)

    //解析完参数后设置默认值
    val pt = options.pt
    val hiveTableName = options.hive_table
    val dbProName = options.db_prop_name
    val dbTableName = options.db_table

    val importMode = options.import_mode match {
      case s: String => s
      case _ => "all"
    }
    val dataDate = options.data_date match {
      case _ if(pt != null)=> CommonUtils.getPtDaysAgo(pt, "yyyyMMdd", 0, "yyyy-MM-dd")
      case _ => options.data_date
    }
    val incCondition = options.import_condition match {
      case s: String => s
      case _ => s"create_time = '$dataDate'"
    }

    println("最终解析得到的参数：")
    println("pt:"+pt)
    println("hiveTable : " + hiveTableName)
    println("dbPropName : " + dbProName)
    println("dbTable : " + dbTableName)
    println("importMode:" + importMode)
    println("incCondition:" + incCondition)




    spark.sparkContext.getConf.setAppName(s"DB2his_$hiveTableName")
    if(!spark.sqlContext.tableNames(hiveTableName.split("\\.")(0)).contains(hiveTableName.split("\\.")(1))){
      //表不存在，创建his表orc格式
      val dbType = DBUtils.getDBType(dbProName)
      val createSql = getCreateSql(dbProName, dbTableName, hiveTableName, dbType)
      spark.sql(createSql)
      println(s"cerated table $hiveTableName")
    }

    //dataframe read from db (增量需要where)
    var df = DBUtils.readFromDB(spark, dbProName, dbTableName).get
    if(importMode == "inc"){
      df = df.where(incCondition)
    }


    // 对比db字段和hive字段不同处进行处理

    // 不一样的字段名，改名

    // db中有，hive中没有的，删除，

    // db没有，hive有（暂时不考虑）
    // db和hive 字段顺序不同（暂时不考虑）




    //insert data
    println("start to insert data ......")
    df.createOrReplaceTempView("tmp")
    pt match {
      case null => spark.sql(s"insert overwrite table $hiveTableName select * from tmp")
      case _ => spark.sql(s"insert overwrite table $hiveTableName partition(pt = '$pt') select * from tmp")
    }

    spark.stop()

  }


  /**
    * 根据字段类型映射关系拼装create语句
    * @param dbProName
    * @param dbTableName
    * @param hiveTableName
    * @param t
    * @return
    */
  def getCreateSql(dbProName: String, dbTableName:String , hiveTableName: String, t: String): String ={
    val (tblComment, colArray) = t match{
      case "mysql" =>
        val dao = MysqlDao(dbProName)
        val csql = dao.getMysqlCreateSql(dbTableName).toLowerCase
        handleMysqlColsAndTblComment(csql, dao)
      case "oracle" =>
        val dao = OracleDao(dbProName)
        val tblComment = dao.getOracleTblComment(dbTableName)
        val cols = dao.getOracleCols(dbTableName)
        Tuple2(tblComment, handleOracleCols(cols, dao))
    }

    db2hiveCreateSql(hiveTableName, tblComment, colArray)
  }

  /**
    * 根据mysql建表语句得到hive建表语句
    * @param sql 建表语句
    * @param dao
    * @return
    */
  def handleMysqlColsAndTblComment(sql: String, dao: AbstractDao): Tuple2[String, List[String]] = {

    // 解析字段
    val reg_colline = "`(\\S+)` (\\S+) (.*comment '(\\S+)')?"r
    val colArray = ArrayBuffer[String]()
    for(m <- reg_colline.findAllMatchIn(sql)){
      colArray += getColsString(m.group(1), dao.db2hiveTypeTran(m.group(2)), m.group(4))
    }

    // 表注释
    val reg_tableComment = """comment='(\S*)'"""r
    val tblComment = reg_tableComment.findFirstMatchIn(sql) match{
      case Some(comment) => comment.group(1)
      case None => ""
    }

    Tuple2(tblComment, colArray.toList)

  }

  /**
    * 根据oralce的表注释，字段组装hive建表语句
    * @param cols
    * @param dao
    * @return
    */
  def handleOracleCols(cols: List[Tuple3[String, String, String]], dao: AbstractDao) ={

    val colArray = ArrayBuffer[String]()
    for (col <- cols){
      colArray += getColsString(col._1, dao.db2hiveTypeTran(col._2), col._3)
    }
    colArray.toList
  }


  /**
    * 组合 字段名字段类型字段注释为建表语句中的一行
    * @param colName
    * @param colType
    * @param colComment
    * @return
    */
  def getColsString(colName: String, colType: String, colComment: String): String = {
    val colCommentString = if(colComment != null && !colComment.isEmpty) "comment '%s'".format(colComment) else ""
    List("  ", colName, colType, colCommentString).mkString(" ")
  }


  /**
    * 抽象出公共的特征组合成hive建表语句
    * @param hiveTableName
    * @param tblComment
    * @param colArray
    * @return
    */
  def db2hiveCreateSql(hiveTableName: String, tblComment: String, colArray: List[String] ): String ={
    // 表名
    val tableString = s"create external table $hiveTableName "
    // 字段格式化
    val colString = colArray.mkString("(\r\n", ",\r\n", "\r\n)")
    // 表注释
    val tableCommentString = if(tblComment != null && !tblComment.isEmpty) "comment '%s'".format(tblComment) else ""
    // 分区&存储
    val tailString = "partitioned by (pt string)\r\nstored as orc;"

    Seq(tableString, colString, tableCommentString, tailString).mkString("\r\n")


  }
}

class DB2HiveArgs(busName: String) extends Args(busName) {
  var hive_table: String= null
  var db_prop_name: String = null
  var db_table: String = null
  var import_mode: String = null
  var import_condition: String = null
  var pt:String = null
  var data_date:String = null
}