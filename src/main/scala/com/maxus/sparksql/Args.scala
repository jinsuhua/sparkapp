package com.maxus.sparksql

import com.maxus.sparksql.util.{CommonUtils, JavaUtils}
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions.seqAsJavaList

/**
  * 参数类
  * @param busName 业务名称 hive2db/db2his
  */
class Args(busName: String) {

  /**
    * 利用反射机制，根据prop属性文件和main传入的参数，来设置最终的参数
    * @param args
    */
  def parseArgs(args: Array[String]) ={
    //解析main参数设置参数中的变量, 第一个为固定的prop，解析其他可变的参数
    var mainMaps = Map[String, List[String]]()
    var key = ""
    var valueBuf = ArrayBuffer[String]()

    for (i <- 1 until args.length){
      val arg = args(i)
      if(arg.startsWith("-")){
        key = arg.substring(1)
        valueBuf.clear()
      }else{
        valueBuf += arg
        mainMaps += (key -> valueBuf.toList)
      }
    }
    println("main传入的参数：")
    println(mainMaps)


    //解析prop文件得到参数设置
    val propName = args(0)
    val props = CommonUtils.getProPerties(busName, propName).get


    //设置参数
    //val params = new Args()

    val fields = this.getClass.getDeclaredFields()
    for (f <- fields){
      f.setAccessible(true)
      val name = f.getName()
      var value = props.getProperty(name)
      if (value != null){
        // prop文件里有这个参数,则覆盖默认参数
        if(mainMaps.contains(name)){
          // main传过来的变量，填充参数
          val list:java.util.List[String] = mainMaps.get(name).get
          value = JavaUtils.fillParamWithArgs(value, list.toArray())
        }
        f.set(this, value)
      }
      //println(f.get(this))
    }


  }
}
