package com.maxus.sparksql.thread

import java.util.concurrent.CyclicBarrier


import scala.collection.mutable.ArrayBuffer

object DB2HisService {
  val spark = SparkInstance.getSparkSession()

  def main(args: Array[String]): Unit = {

    val businessName = args(0)
    val batch = args(1)

    val pt = batch.length match {
      case 10 => batch.substring(0,8)
      case _ => batch
    }

    // 检查批次有没有生成
    var hisList: List[HisInstance] = Nil
    DBClient.getHisInstanceByBatch(businessName, batch) match {
      case hs: List[HisInstance] if hs.length > 0 =>
        println(s"[Main] [HisInstance批次${batch}已存在]")
        // 取出 ready 和 error 的重跑
        hisList = hs.filter(a =>a.status == "ready" || a.status == "error")
      case _ =>
        // 生成his 批次，状态为ready
        hisList = DBClient.initHisBatch(businessName, batch)
    }

    println("[Main] [Start] [抽表任务开始]")

    if(hisList != null && hisList.length > 0){
      doCyclicBarrier(hisList, pt)
    }

    // 集聚点后检查是不是都是finish状态，否就退出系统，是就继续后面任务
    if(!DBClient.checkIfHisAllFinish(businessName, batch)){
      println(s"[Main] [${batch} 抽表任务有未完成的，退出系统]")
      DBClient.closeConn()
      System.exit(-1)
    }

    println("[Main] [Finish] [抽表任务执行完毕]")
  }


  def doCyclicBarrier(threadList: List[HisInstance], pt: String): Unit = {
    val cb = new CyclicBarrier(threadList.length + 1)
    threadList.foreach(tl => new Thread(new MyRunable(cb, tl, pt)).start())
    cb.await()
  }

  def db2his(hiveTable: String, dbProp: String, dbTable: String,
             whereCondition:String, diffCols: String, pt: String): Unit = {

    val where = whereCondition match {
      case s:String if s.trim.length > 0 =>
        if(s.contains("#{pt}"))
          s.replace("#{pt}", pt)
        else
          s
      case _ => "1 = 1"
    }

    // 从db读取数据得到df
    val tmpdf = CommonUtil.readFromDB(dbProp, dbTable, where).get
    val tmpTable = hiveTable.split("\\.")(1)
    tmpdf.createOrReplaceTempView(tmpTable)

    // 得到hive中的字段
    val rows = spark.sql(s"desc $hiveTable").collect()
    // 是否 是分区表
    val ifPartitionTable = rows.contains(org.apache.spark.sql.Row("# Partition Information","",""))
    val cols_arr: Array[String] = rows.filter(row => !row.getString(0).startsWith("#")).map(_.getString(0))

    var cols_tmp = ArrayBuffer[String]()
    var colNames = ArrayBuffer[String]()
    for (field <- cols_arr) {
      if (cols_tmp.contains(field)) {
        // 去掉分区字段
        colNames = colNames.filter(!_.equals(field))
      } else {
        cols_tmp += field
        colNames += field
      }
    }

    // 字段名不一样时候的映射
    if(diffCols != null && diffCols.trim.length>0){
      var mappingColsMap:Map[String, String] = Map.empty
      diffCols.split("\\|").map(col =>
        mappingColsMap += (col.split(":")(0)-> col.split(":")(1)))
      colNames = colNames.map(col => if(mappingColsMap.contains(col)) s"${mappingColsMap.get(col).get} as col" else col)
    }

    // 去掉分区字段后的hive表里的col组合
    val colStr = colNames.mkString(", ")

/*    var ifDistinct = ""
    if(List("smcv_sel.v_dol_dealer_retail_new", "smcv_sel.v_dol_dealer_wholesale", "smcv_sel.v_dol_company_wholesale").contains(dbTable)){
      ifDistinct = "distinct"
      spark.
    }*/

    val sqlCommon = ifPartitionTable match {
      case true =>
        s"insert overwrite table ${hiveTable} partition(pt='${pt}') select ${colStr} from ${tmpTable}"
      case false =>
        s"insert overwrite table ${hiveTable} select ${colStr} from ${tmpTable}"
    }
    val sql =hiveTable match {
      case "smcv.mes_tt_qas_checkout_his" =>
        s"insert overwrite table ${hiveTable} partition(pt='${pt}', loc) select * from ${tmpTable}"
      //case "devdb.bi_offline_national_iv_standard_vin_his" =>
      //  s"insert overwrite table ${hiveTable} select * from ${tmpTable}"
      case ht: String if List("smcv_sel.v_dol_dealer_retail_new", "smcv_sel.v_dol_dealer_wholesale", "smcv_sel.v_dol_company_wholesale").contains(dbTable) =>
        val sql1 = s"insert overwrite table ${hiveTable} partition(pt) select distinct ${colStr},regexp_replace(op_date,'-','') as pt from ${tmpTable} t"
        println(sql1)
        sql1
      case _ => sqlCommon
    }
    spark.sparkContext.setJobDescription(s"db2his -> $hiveTable")
    //spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql(sql)
  }


  class MyRunable(cb: CyclicBarrier, his: HisInstance, pt: String) extends Runnable{
    override def run(): Unit = {

      val th = Thread.currentThread()

      // db和hive字段名不一样时，映射关系配置，多个之间竖线分隔  hive字段名:db字段名
      val diffCols = his.diffCols
      val hiveTable = his.hiveTable.trim
      val dbProp = his.dbProp
      val dbTable = his.dbTable.trim
      // 抽表的where条件
      val whereCondition = his.whereCondition

      try {
        println(s"[MyRunable] $th 开始抽表：$dbTable")
        // 抽表
        db2his(hiveTable, dbProp, dbTable, whereCondition, diffCols, pt)
        println(s"[MyRunable] $th 结束抽表：$dbTable")
        // 设置为finish状态
        DBClient.updateHisStatus(his.businessName, his.batch, hiveTable, "finish")
        cb.await()

      } catch{
        // 抽表异常，设置为error状态
        case e: Exception =>
          e.printStackTrace()
          println(s"[MyRunable] [ERROR] $th 抽表失败：$dbTable")
          DBClient.updateHisStatus(his.businessName, his.batch, hiveTable, "error")
          cb.await()
      }

    }

  }
}
