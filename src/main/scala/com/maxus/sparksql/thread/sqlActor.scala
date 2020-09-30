package com.maxus.sparksql.thread

import java.util.Date

import akka.actor.{Actor, Props}
import akka.routing.RoundRobinPool
import com.sun.xml.internal.bind.v2.TODO
import org.apache.spark.sql.DataFrame

import scala.io.Source
import com.typesafe.scalalogging.Logger
import akka.event.Logging


class SqlMasterActor extends Actor{

  //val logger = Logging(context.system, this)

  val workerRouter = context.actorOf(
    Props[SqlWorkerActor].withRouter(RoundRobinPool(10)), name = "workerRouter")

  // 记录错误次数列表
  val errorList = scala.collection.mutable.Map[Long, Int]()
  // 正在运行的id列表
  val idWaitToRun = scala.collection.mutable.Set.empty[Long]
  // sql需要替换的参数
  var params: Map[String, String] = _
  var businessName: String = _
  var batch: String = _

  def receive = {

    case "schedual" =>
      // 查找状态为ready和failed的任务放到idWaitToRun里面，发送给workerRouter执行
      DBClient.getInstanceByStatus(businessName, batch, "ready").foreach{ instance =>
       val instanceId = instance.tid
       idWaitToRun contains instanceId match {
         case false =>
           idWaitToRun.add(instanceId)
           workerRouter ! (params, instance)
         case true =>
       }

        println(s"[Master] [doingList ${idWaitToRun}]")
      }

    case status_instanceId: (String, Long) =>
      val status = status_instanceId._1
      val instanceId = status_instanceId._2
      status match {
        case "error" =>

          // 任务重试，失败4 次后退出
          if(errorList.contains(instanceId)){
            var time = errorList(instanceId)
            if(time < 2){
              time += 1
              errorList += (instanceId -> time)
            }else{
              DBClient.updateStatus(businessName, batch, instanceId, "error")
              println("[Master] [instanceId ${instanceId}] [任务失败次数达到上限，即将退出系统服务]")
              DBClient.closeConn()
              this.context.system.terminate()
            }
          }else{
            errorList += (instanceId -> 1)
          }
          println(s"[Master] [instanceId ${instanceId}] [失败次数${errorList(instanceId)}]")

          // 设置ready状态进行重试
          DBClient.updateStatus(businessName, batch, instanceId, "ready")
        case "finish" =>
          // 设置状态为finish，并移除
          DBClient.updateStatus(businessName, batch, instanceId, "finish")

          // 查找compeleteInstanceId 的下游任务是否满足执行条件（所有上游任务都完成）, 满足条件就设置状态为ready
          val children = DBClient.getAllChildren(batch, instanceId, businessName)
          //没有下游任务的话， 判断所有任务是否全部完成
          if(children == null || children.length == 0 ){
            DBClient.checkIfAllInstanceDone(businessName, batch) match {
              case true =>
                println("[Master] [所有任务执行完毕，退出系统服务]")
                //TODO 把finish的任务移动至已完成的表里
                println(s"-------------------------------- 业务名称：$businessName 业务批次：$batch  结束运行 --------------------------------")
                DBClient.closeConn()
                this.context.system.terminate()
              case false =>
            }
          }

          children.foreach{ instance =>
              val tid = instance.tid
              DBClient.checkIfAllParentDone(batch, tid, businessName) match{
                case true =>
                  println(s"[Master] [${tid} 的所有父亲任务已完成，设置为ready状态等待执行]")
                  // 多个任务可能同时完成到达这里，其他任务可能把它已经设置为ready了或者runing起来了，所以update时候只能把init状态的设置为ready
                  DBClient.updateStatus(businessName, batch, tid, "ready", "init2ready")
                case false =>
                  println(s"[Master] [${tid} 的所有父亲任务中有未完成的]")
              }
          }
      }

      idWaitToRun remove instanceId

    case business_batch_params: (String, String, Map[String, String]) =>
      // 初始化
      businessName = business_batch_params._1
      batch = business_batch_params._2
      params = business_batch_params._3

      println(s"-------------------------------- 业务名称：$businessName 业务批次：$batch  开始运行 --------------------------------")
      println(s"参数为 $params")

    case _ =>
      println("can`t match any receive")
  }
  
  override def preStart(): Unit = {
    //println("sqlManager start")
  }

  override def postStop(): Unit = {
    //println("sqlManager stop")
  }
}


class SqlWorkerActor extends Actor  {
  //val logger = Logging(context.system, this)




  def receive = {

    case params_instance: (Map[String, String], TaskInstance) =>
      val paramsMap = params_instance._1
      val instance = params_instance._2
      try{
          // 设置状态为running
          DBClient.updateStatus(instance.businessName, instance.batch, instance.tid, "running")
          println(s"[Worker] [START] [instanceId ${instance.tid}] [time ${DBClient.nowDate()}]")
          SparkInstance.getSparkSession.sparkContext.setJobDescription(instance.name)

          if(instance.name.endsWith(".sql")){
            handleSql(instance, paramsMap)
          }else if (instance.name.endsWith(".properties")){
            handleDX(instance.name, paramsMap("pt"))
          }

          println(s"[Worker] [FINISH] [instanceId ${instance.tid}] [time ${DBClient.nowDate()}]")
          // 完成任务返回给master
          sender ! ("finish", instance.tid)

      } catch {
        case e: Exception =>
          e.printStackTrace()
          println(s"[Master] [ERR]  [instanceId ${instance.tid}] [time ${DBClient.nowDate()}] [$e]")
          sender ! ("error", instance.tid)
      }

    case _ =>
      println("can`t match any receive")

   }


  /**
    * 替换变量
    * @param sqlRun
    * @param paramsMap
    * @param instanceId
    * @return
    */
  def replaceVar(sqlRun: String, paramsMap: Map[String, String], instanceId: Long): String = {
    var sqlRunFinal = sqlRun
    val reg = """\$\{\S+\}"""r
    val reg1 = """(\$[^{]\S+?)'?"""r
    var regSet = Set.empty[String]

    // 能匹配到的 ${xx} $xx, 都到set里
    for(m <- reg.findAllMatchIn(sqlRun)){
      regSet += m.group(0)
    }
    for(m <- reg1.findAllMatchIn(sqlRun)){
      regSet += m.group(1)
    }

    // 替换set中的变量
    if(regSet.nonEmpty){
      for(s <- regSet){
        val param = s.replace("$","").replace("{","").replace("}","")
        //println(s"paramsMap: $paramsMap")
        if(!paramsMap.contains(param)){
          println(s"[Worker] [ERR]  [instanceId ${instanceId}] [time ${DBClient.nowDate()}] [$s 参数未设置]")
          sender ! ("error", instanceId)
        }
        //println(s"[Worker] [instanceid ${instance.tid}] [替换${s} 为 ${paramsMap(param)}]")
        sqlRunFinal = sqlRunFinal.replace(s, paramsMap(param))
      }
    }
    sqlRunFinal
  }


  /**
    * 处理sql文件
    * @param instance
    * @param paramsMap
    */
  def handleSql(instance: TaskInstance, paramsMap: Map[String, String]) ={


    // 读取文件内容
    val fileBuf = Source.fromFile(TaskRelationService.SQL_DIR + instance.name).getLines()

    // 文件中解析出来的sql组装
    var sql = ""
    for (line <- fileBuf) {
      if(line.trim().startsWith("check_partition")){
        val checks = line.split(":")(1).trim.split(" ")
        val db = checks(0)
        val table = checks(1)
        val partition = checks(2)
        sql = s"show partitions ${db}.${table} partition(${partition})"
        // 替换变量
        sql = replaceVar(sql, paramsMap, instance.tid)
        println(s"[Worker] [instanceId ${instance.tid}] [$sql]")
        val df = runSql(sql)
        df.collect().length match{
          case 0 =>
            println(s"[Worker] [ERR]  [instanceId ${instance.tid}] [time ${DBClient.nowDate()}] [check_partition(${db}.${table} ${partition}) error]")
            sender ! ("error", instance.tid)
          case _ =>
        }
        sql = ""
      }
      //sql 语句
      else if (!line.trim().startsWith("--") && line.trim() != "") {
        if(line.indexOf("--") > 0 ){
          sql += s"\r\n${line.substring(0, line.indexOf("--"))}"
        }else{
          sql += s"\r\n${line}"
        }
        if (sql.trim().endsWith(";")) {
          // 需要运行的sql语句
          var sqlRun = sql.trim.dropRight(1)

          // 替换变量
          sqlRun = replaceVar(sqlRun, paramsMap, instance.tid)

          if(!sqlRun.trim.equals("") && !sqlRun.contains("spark.sql.shuffle.partitions")){
            // 最终替换参数后的sql语句
            val sqlRunPrint = sqlRun.length match{
              case len:Int if len < 101=> sqlRun
              case _ => s"${sqlRun.substring(0,100)}..."
            }
            println(s"[Worker] [instanceId ${instance.tid}] running sql: $sqlRunPrint")
            //spark 运行sql
            //Thread.sleep(instance.tid * 5000)
            runSql(sqlRun)
          }

          sql = ""
        }
      }

    }
  }


  /**
    * 处理dx任务
    * @param instanceName
    * @param pt
    */
  def handleDX(instanceName: String, pt: String) ={

    // 解析文件
    val prop = CommonUtil.getDXPerties(instanceName).get

    val hiveTable = prop.getProperty("hive_table")
    val dbProp = prop.getProperty("db_prop")
    val dbTable = prop.getProperty("db_table")
    val whereCondition = prop.getProperty("where_condition")
    val diffCols = prop.getProperty("diff_cols")

    DB2HisService.db2his(hiveTable, dbProp, dbTable, whereCondition, diffCols, pt)
  }

   
  override def preStart(): Unit = {
    //println("sqlWorker start")
  }

  override def postStop(): Unit = {
    //println("sqlWorker stop")
  }

  def runSql(sql: String): DataFrame={
    SparkInstance.getSparkSession().sql(sql)
  }

}
