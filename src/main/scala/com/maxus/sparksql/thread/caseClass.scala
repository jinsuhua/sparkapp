package com.maxus.sparksql.thread

case class SqlJob(id:Long, sql:String, path:String, yymmdd:String)
case class SqlResult(id:Long, count:Option[Long], exception:Option[Exception])


case class His(hiveTable: String, dbProp: String, dbTable: String, whereCondition:String, diffCols: String, businessName: String)
case class HisInstance(businessName: String, batch: String, hiveTable: String, dbProp: String, dbTable: String, whereCondition:String, diffCols: String, status: String, createTime:String, updateTime: String)

case class Task(tid: Long, name: String, businessName: String)
case class TaskRelation(tid: Long, parentId: Long, businessName: String)
case class TaskInstance(businessName: String, batch: String, tid: Long, name: String, status: String, createTime:String, updateTime: String, startTime: String, finishTime: String)
