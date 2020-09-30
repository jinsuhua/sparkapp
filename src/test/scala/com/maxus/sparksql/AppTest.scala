package com.maxus.sparksql

import java.text.SimpleDateFormat
import java.util.Calendar

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object AppTest {

  def main(args: Array[String]): Unit = {

    //combileCols("data_date:to_date('2019-10-28','yyyy-MM-dd')")
    //print("smcv.xxx".split("\\.")(1))
   /* val a = "select * from test where pt = $pt_1  and pt = '${pt}'"
    val aa = "select * from test where pt='${pt_1_day_ago}' and pt = $pt and pt = ${pt} and pt= $pt"
    //println(s"$aa")

    val reg = """\$\{\S+\}"""r
    val reg1 = """\$[^{]\S+"""r

    //reg.findAllMatchIn(aa)
    val colArray = ArrayBuffer[String]()
    for(m <- reg.findAllMatchIn(aa)){
      println(m.group(0))
    }
    for(m <- reg1.findAllMatchIn(aa)){
      println(m.group(0))
    }*/

/*    val reg = """\$\{\S+\}"""r
    val reg1 = """\$[^{]\S+"""r
    var buf = Source.fromFile("D:/insert_dol_demand_mobile_board_mes.sql")
    var sql = ""
    for (a <- buf.getLines()){
      if(!a.trim().startsWith("--")){
        sql += s"\r\n$a"
        if(sql.endsWith(";")){
          println("============"+ sql.dropRight(1))
          sql = ""
        }
      }
    }*/


    /*var str = Source.fromFile("D:/insert_dol_demand_mobile_board_mes.sql").mkString
    //print(str)
    for(m <- reg.findAllMatchIn(str)){
      str = str.replace(m.group(0), "20191112")
    }
    for(sql <- str.split(";")){
      println(sql)
    }*/
    /*var params = Map("pt" -> "20191126")
    params += ("pt_1_day_ago" -> "20191125")
    params += ("data_date" -> "2019-11-26")
    params += ("date_90min_later" -> "2019-11-26 13:30:00")
    handleSql(params)*/

    val m = Map((1,"a"),(2,"b"))
    m.foreach( m => println(m._1))
  }


  def handleSql( paramsMap: Map[String, String]) ={

    // 读取文件内容
    val fileBuf = Source.fromFile("D:\\doc\\week_task\\ETL\\shell_sql\\spark\\sql\\insert_dol_stock_trans_claim_mes.sql").getLines()

    // 文件中解析出来的sql组装
    var sql = ""
    for (line <- fileBuf) {
      if(line.trim().startsWith("check_partition")){
        val checks = line.split(":")(1).trim.split(" ")
        val db = checks(0)
        val table = checks(1)
        val partition = checks(2)
        sql = s"show partitions ${db}.${table} partition(${partition})"
        sql = replaceVar(sql, paramsMap, 111)
        println(sql)
        //val df = runSql(sql)
        /*df.collect().length match{
          case 0 =>
            println(s"[Worker] [ERR]  [instanceId ${instance.tid}] [time ${DBClient.nowDate()}] [check_partition(${db}.${table} ${partition}) error]")
            sender ! ("error", instance.tid)
          case _ =>
        }*/
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
          //println(sqlRun)
          if(!sqlRun.trim.equals("") && !sqlRun.contains("spark.sql.shuffle.partitions")) {
            sqlRun = replaceVar(sqlRun, paramsMap, 111)

            // 最终替换参数后的sql语句
            val sqlRunPrint = sqlRun.length match {
              case len: Int if len < 101 => sqlRun
              case _ => s"${sqlRun.substring(0, 100)}..."
            }
            println(s"running sql: $sqlRun")
          }
          //spark 运行sql
          //Thread.sleep(instance.tid * 5000)
          //runSql(sqlRun)

          // 模拟
          /*Thread.sleep(instance.tid * 5000)
          if(instance.tid == 3){
            1/0
          }*/

          // 清空 进行下一次循环
          //regSet = Set.empty[String]
          sql = ""
        }
      }

    }
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
    println("xxxxxxxxxxxxxxxx" + regSet)

    // 替换set中的变量
    if(regSet.nonEmpty){
      for(s <- regSet){
        val param = s.replace("$","").replace("{","").replace("}","")
        //println(s"paramsMap: $paramsMap")
        if(!paramsMap.contains(param)){
          println(s"[Worker] [ERR]  [instanceId ${instanceId}]  [$s 参数未设置]")
          //sender ! ("error", instanceId)
        }
        //println(s"[Worker] [instanceid ${instance.tid}] [替换${s} 为 ${paramsMap(param)}]")
        sqlRunFinal = sqlRunFinal.replace(s, paramsMap(param))
      }
    }
    sqlRunFinal
  }

}


