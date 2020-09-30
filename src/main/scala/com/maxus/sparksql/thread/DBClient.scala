package com.maxus.sparksql.thread

import java.io.FileInputStream
import java.sql.{Connection, DriverManager, ResultSet}
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Properties}

import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.mutable.ArrayBuffer

object DBClient{

  var conn: Connection = _
  private val dbProName = "horae"
  //private val spark = SparkInstance.getSparkSession()
  private val TABLE_TASK = "platform.jsh_task"
  private val TABLE_TASK_RELATION = "platform.jsh_task_relation"
  private val TABLE_TASK_INSTANCE = "platform.jsh_task_instance"
  private val TABLE_HIS = "platform.jsh_his"
  private val TABLE_HIS_INSTANCE = "platform.jsh_his_instance"




  /**
    * 得到数据库连接
    * @return
    */
  def getConn(): Connection = {
    if(conn == null) {
      println("获取数据库连接开始")
      val properties = getDBProperties().get
      properties.setProperty("charset", "UTF-8")
      val driver = properties.getProperty("driver")
      val url = properties.getProperty("url")
      Class.forName(driver)
      conn = DriverManager.getConnection(url, properties)
      println("获取数据库连接结束")
    }
    conn
  }

  /**
    * 关闭连接
    */
  def closeConn(): Unit ={
    println("数据库连接关闭")
    if(conn != null){
      conn.close()
    }
  }

  /**
    * 根据属性文件名称得到得到属性properties对象
    * @return
    */
  def getDBProperties(): Option[Properties] = {
    val dir = s"/home/smcv/shell_sql/spark/conf/db/${dbProName}.properties"
    val st = new FileInputStream(dir)
    val properties = new Properties()
    try{
      properties.load(st)
      Some(properties)
    }catch{
      case e:Exception =>
        val mes = e.getMessage
        throw new Exception(s"$mes -- ${dbProName}.properties not found")
    }
  }

  /**
    * 得到map形式的属性
    *
    * @param dbtable
    * @return
    */
  def getDBProMap(dbtable: String): Map[String, String] ={
    val properties = getDBProperties().get
    var options = Map[String, String]()
    options += "url" -> properties.getProperty("url")
    options += "user" -> properties.getProperty("user")
    options += "password" -> properties.getProperty("password")
    options += "dbtable" -> dbtable
    options += "driver" -> properties.getProperty("driver")
    options
  }

  def nowDate(): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = dateFormat.format(now)
    return date
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
      //if (conn != null) conn.close()
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
    try {
      val retSet = sm.executeQuery()
      val ret = f(retSet)
      if(retSet != null) retSet.close()
      Some(ret)
    }finally {
      if(sm != null) sm.close()
      //if(conn !=null) conn.close()
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
    * 得到instance实例集合
    * @param retSet
    * @return
    */
  def getTaskInstanceFun(retSet: ResultSet): List[TaskInstance] ={
    val buf = ArrayBuffer[TaskInstance]()
    while (retSet.next()) {
      buf += TaskInstance(
        retSet.getString(1),
        retSet.getString(2),
        retSet.getLong(3),
        retSet.getString(4),
        retSet.getString(5),
        retSet.getString(6),
        retSet.getString(7),
        retSet.getString(8),
        retSet.getString(9)
      )
    }
    buf.toList
  }


  /**
    * 得到his instance实例集合
    * @param retSet
    * @return
    */
  def getHisInstanceFun(retSet: ResultSet): List[HisInstance] ={
    val buf = ArrayBuffer[HisInstance]()
    while (retSet.next()) {
      buf += HisInstance(
        retSet.getString(1),
        retSet.getString(2),
        retSet.getString(3),
        retSet.getString(4),
        retSet.getString(5),
        retSet.getString(6),
        retSet.getString(7),
        retSet.getString(8),
        retSet.getString(9),
        retSet.getString(10)
      )
    }
    buf.toList
  }

  /**
    * 初始化instance表某一批次实例，状态设置为init
    * @param businessName
    * @param batch
    */
  def initInstanceBatch(businessName: String, batch: String): Unit = {
    val now =nowDate()
    val sql = s"insert into ${TABLE_TASK_INSTANCE} select '${businessName}', '${batch}', tid, name, 'init', '${now}', '${now}', null, null from ${TABLE_TASK} where business_name='${businessName}'"
    exeSql(sql)
  }
  /*def initInstanceBatch(businessName: String, batch: String): Unit = {
    val whereCondition = s"business_name='${businessName}'"
    val TaskDF = readFromDB(TABLE_TASK, whereCondition).get
    val instanceList = new util.ArrayList[Instance]()
    TaskDF.foreach{ row =>
      instanceList.add(
        Instance(
          batch,
          row.getLong(1),
          "init",
          nowDate(),
          nowDate(),
          null, null
        )
      )
    }
    import spark.implicits._
    val df = spark.createDataset(instanceList).toDF()
    writeToDB(df, TABLE_INSTANCE, SaveMode.Append)
  }*/

  /**
    * 更新状态
    * @param businessName
    * @param batch
    * @param tid
    * @param status
    */
  def updateStatus(businessName:String, batch: String, tid: Long, status: String, condition: String = ""): Unit ={
    val now = nowDate()
    val pre = s"update ${TABLE_TASK_INSTANCE} set status = '${status}', update_time = '${now}'"
    var end = s"where business_name='${businessName}' and batch = '${batch}' and tid = ${tid}"
    condition match {
      case "init2ready" => end += s" and status = 'init'"
      case _ =>
    }
    val mid = status match {
      case "ready" => s", start_time='${now}'"
      case "finish" => s", finish_time='${now}'"
      case _ => ""
    }
    val sql = pre.concat(mid).concat(end)
    exeSql(sql)
  }

  /**
    * 检查是否全部父任务已完成
    * @param batch
    * @param tid
    * @return
    */
  def checkIfAllParentDone(batch: String, tid: Long, businessName: String): Boolean = {
    val sql = s"select * from ${TABLE_TASK_INSTANCE} where batch = '${batch}' and tid in (select parent_id from ${TABLE_TASK_RELATION}  where tid =${tid} and business_name='${businessName}') and status != 'finish'"
    val flag = getQueryResult(sql, IfResultFun).get.head match {
      case s: Boolean => !s
    }
    flag
  }

  /**
    * 检查是否所有指定批次任务已完成
    * @param businessName
    * @param batch
    * @return
    */
  def checkIfAllInstanceDone(businessName: String, batch: String): Boolean = {
    val sql = s"select * from ${TABLE_TASK_INSTANCE} a join ${TABLE_TASK}  b on a.business_name = b.business_name and a.tid = b.tid where a.business_name='${businessName}' and a.batch = '${batch}' and a.status != 'finish'"
    val flag = getQueryResult(sql, IfResultFun).get.head match {
      case s: Boolean => !s
    }
    flag
  }

  /**
    * 重跑时候设置runing和error状态的任务为ready
    * @param businessName
    * @param batch
    */
  def setRunningAndError2Ready(businessName: String, batch: String): Unit = {
    val now = nowDate()
    val sql = s"update ${TABLE_TASK_INSTANCE} set status = 'ready', start_time='${now}', update_time='${now}' where business_name='${businessName}' and batch = '${batch}' and status in('error','running')"
    exeSql(sql)
  }


  /**
    * 服务开始时候查找本次批次
    * @param businessName
    * @param batch
    * @return
    */
  def checkTaskInstanceBatch(businessName: String, batch: String): List[TaskInstance] = {
    val sql = s"select * from ${TABLE_TASK_INSTANCE} where business_name='${businessName}' and batch = '${batch}'"
    val ins = getQueryResult(sql, getTaskInstanceFun).get match {
      case list: List[TaskInstance] => list
    }
    ins
  }


  /**
    * 得到所有子任务
    * @param batch
    * @param tid
    * @return
    */
  def  getAllChildren(batch: String, tid: Long, businessName: String): List[TaskInstance] ={

    val sql = s"select * from ${TABLE_TASK_INSTANCE} where business_name = '${businessName}' and batch = '${batch}' and tid in (select tid from ${TABLE_TASK_RELATION} where parent_id = ${tid} and business_name='${businessName}')"
    val ins = getQueryResult(sql, getTaskInstanceFun).get match {
      case list: List[TaskInstance] => list
    }
    ins
  }

  /**
    * 初始化设置头部任务状态为ready
    * @param businessName
    * @param batch
    */
  def setTopTaskReady(businessName: String, batch: String): Unit = {
    val sql = s"update ${TABLE_TASK_INSTANCE} set status='ready',start_time='${nowDate()}' where business_name = '${businessName}' and batch='${batch}' and tid in (select a.tid from ${TABLE_TASK_RELATION} a left join ${TABLE_TASK} b on a.business_name = b.business_name and a.tid=b.tid where a.business_name='${businessName}' and a.parent_id is null)"
    exeSql(sql)
  }

  /**
    * 根据状态找到任务
    * @param businessName
    * @param batch
    * @param status
    * @return
    */
  def getInstanceByStatus(businessName: String, batch: String, status: String): List[TaskInstance] ={
    val sql = s"select * from ${TABLE_TASK_INSTANCE} a left join ${TABLE_TASK} b on a.business_name = b.business_name and a.tid = b.tid where a.business_name='${businessName}' and a.batch='${batch}' and a.status='${status}'"
    val ins = getQueryResult(sql, getTaskInstanceFun).get match {
      case list: List[TaskInstance] => list
    }
    ins
  }

  /**
    * 抽表任务开始时候检查抽表批次
    * @param businessName
    * @param batch
    * @return
    */
  def getHisInstanceByBatch(businessName: String, batch: String): List[HisInstance] ={
    val sql = s"select * from ${TABLE_HIS_INSTANCE} where business_name = '${businessName}' and batch = '${batch}'"
    val ins = getQueryResult(sql, getHisInstanceFun).get match {
      case list: List[HisInstance] => list
    }
    ins
  }

  /**
    * 初始化hisInstance 批次
    * @param businessName
    * @param batch
    */
  def initHisBatch(businessName: String, batch: String): List[HisInstance] ={
    val now =nowDate()
    val sql = s"insert into ${TABLE_HIS_INSTANCE} select '${businessName}', '${batch}', hive_table, db_prop, db_table, where_condition, diff_cols, 'ready', '${now}', '${now}' from ${TABLE_HIS} where business_name='${businessName}'"
    exeSql(sql)
    getHisInstanceByBatch(businessName, batch)
  }


  /**
    * 检查his批次是否都已经完成
    * @param businessName
    * @param batch
    * @return
    */
  def checkIfHisAllFinish(businessName: String, batch: String): Boolean ={
    val sql = s"select * from ${TABLE_HIS_INSTANCE} where business_name = '${businessName}' and batch = '${batch}' and status != 'finish'"
    val flag = getQueryResult(sql, IfResultFun).get.head match {
      case s: Boolean => !s
    }
    flag
  }


  /**
    * 更新his实例状态
    * @param businessName
    * @param batch
    * @param hiveTable
    * @param status
    */
  def updateHisStatus(businessName: String, batch: String, hiveTable: String, status: String): Unit ={
    val sql = s"update ${TABLE_HIS_INSTANCE} set status = '${status}', update_time='${nowDate()}' where business_name = '${businessName}' and batch = '${batch}' and hive_table = '${hiveTable}'"
    //println(sql)
    exeSql(sql)
  }




}

