package com.maxus.sparksql

import java.util.concurrent.CyclicBarrier

object ThreadTest {

  def main(args: Array[String]): Unit = {
    val sql1 = "select 1;"
    val sql2 = "select 2;"
    val sql3 = "select 3;"
    val sql4 = "select 4;"
    val sql5 = "select 5;"
    val sql6 = "select 6;"
    val sql7 = "select 7;"

    val t1 = sql1 :: sql2 :: Nil
    val t2 = sql3 :: Nil
    val t3 = sql4 :: sql5 :: Nil
    val t4 = sql6 :: Nil
    val t5 = sql7 :: Nil


    val threadlist1 = t1 :: t2 :: Nil
    doCyclicBarrier(threadlist1)
    println("-------------------threadlist1执行完毕")

    val threadlist2 = t3 :: Nil
    doCyclicBarrier(threadlist2)
    println("-------------------threadlist2执行完毕")


    val threadlist3 = t4 :: t5 :: Nil
    doCyclicBarrier(threadlist3)
    println("-------------------threadlist3执行完毕")

    println("-------------------所有thread执行完毕")
  }

  def doCyclicBarrier(threadlist: List[List[String]]): Unit = {
    val cb = new CyclicBarrier(threadlist.length + 1)
    threadlist.foreach(tl => new Thread(new MyRunable(cb, tl)).start())
    cb.await()
  }


  class MyRunable(cb: CyclicBarrier, sqllist: List[String]) extends Runnable{
    override def run(): Unit = {
      val th = Thread.currentThread()
      println(s"开始执行sql线程$th-------------------------------------")
      for (sql <- sqllist){
        println(s"$th run sql: $sql")
        //val df = spark.sql(sql)
        //df.show()
      }
      println(s"结束执行sql线程$th-------------------------------------")
      cb.await()
    }
  }
}
