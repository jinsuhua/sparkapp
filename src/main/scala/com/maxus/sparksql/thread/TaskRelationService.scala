package com.maxus.sparksql.thread


import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration.Duration
import com.typesafe.scalalogging.Logger



object TaskRelationService {
  val SQL_DIR = "/home/smcv/shell_sql/spark/sql/"


  def main(args: Array[String]): Unit = {
    //val logger = Logger(this.getClass)

    val businessName = args(0)
    val pt_all = args(1)

    val batch = pt_all.length match {
      case 14 => pt_all.substring(0,10)
      case _ => pt_all
    }


    // 解析参数,不同业务需要的参数不同，默认需要pt，pt_1_day_ago, data_date
    val paramsMapAll = CommonUtil.getParamsMap(pt_all)

    val paramsMap = businessName match{
      case "sales_report" => paramsMapAll
      case _ => paramsMapAll.filter(p => p._1 == "pt" || p._1 == "pt_1_day_ago" || p._1 == "data_date")
    }



    //val props = new Properties()
    //props.load(getClass().getResourceAsStream("/conf/mylog4j.properties"))
    //PropertyConfigurator.configure(props)



    // 检查并初始化任务依赖的数据库的批次
    DBClient.checkTaskInstanceBatch(businessName, batch) match {
      case ins :List[TaskInstance] if ins.length>0 =>
        println(s"[Main] 批次${batch}已存在")
        if(ins.forall(p => p.status == "finish")){
          //全部为finish状态，结束服务
          println(s"[Main] ${batch}任务都已完成，退出系统")
          DBClient.closeConn()
          System.exit(0)
        }else if(ins.forall(p => p.status == "init")){
          //全部为init状态
          // 设置头任务为ready状态，准备工作
          println(s"[Main] 开始设置头任务为ready状态")
          DBClient.setTopTaskReady(businessName, batch)
        }else{
          println("[Main] 设置running和error状态的任务为ready，准备重跑任务")
          // 说明为重跑任务，设置running和error状态的任务为ready，准备重跑
          DBClient.setRunningAndError2Ready(businessName, batch)
        }
      case _ =>
        // 生成任务某一批次实例, 状态为init
        println(s"[Main] 开始生成批次[${batch}]任务")
        DBClient.initInstanceBatch(businessName, batch)

        // 设置头任务为ready状态，准备工作
        println(s"[Main] 开始设置头任务为ready状态")
        DBClient.setTopTaskReady(businessName, batch)
    }

    //初始化actor system
    val conf = ConfigFactory.load("actor")
    val system = ActorSystem("SalesDailyReport-service",conf)
    val sqlMasterActor = system.actorOf(Props[SqlMasterActor], name="SqlMasterActor")

    // 批次信息，参数 事先传过去
    sqlMasterActor ! (businessName, batch, paramsMap)

    import system.dispatcher
    system.scheduler.schedule(Duration.create(5, "second") , Duration.create(1, "second"),sqlMasterActor,"schedual")



    //spark.stop

  }



}
