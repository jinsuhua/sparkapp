package com.maxus.sparksql

import com.maxus.sparksql.util.{DBUtils, CommonUtils, SQLUtils}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * Hello world!
 *
 */
object MesMapNj extends SparkSessionBase {
  def main(args: Array[String]): Unit = {

    //val pt = "201904251955"
    val pt = args(0)
    val funWithPtFormat = CommonUtils.getPtDaysAgo(pt, "yyyyMMddHHmm", _:Int, _:String)
    val data_date = funWithPtFormat(0, "yyyy-MM-dd HH:mm:00")
    val pt_1_day_ago = funWithPtFormat(1, "yyyyMMdd")
    val data_date_90_day_ago = funWithPtFormat(90, "yyyy-MM-dd")
    //val spark = SparkSession.builder().appName("mes_map_nj_5_min_task").enableHiveSupport().getOrCreate()
    spark.sparkContext.getConf.setAppName("mes_map_nj_5_min_task")

    // read from oracle
    val mes_tm_ofm_order_nj_his = "mes_tm_ofm_order_nj_his"
    val mes_tm_ofm_order_nj_hb_his_df = DBUtils.readFromDB(spark, "smcvogg", "NJ_IMES_OWAPUSR01.TM_OFM_ORDER").get
    mes_tm_ofm_order_nj_hb_his_df.createOrReplaceTempView(mes_tm_ofm_order_nj_his)
    val mes_tm_vhc_vehicle_nj_his = "mes_tm_vhc_vehicle_nj_his"
    val mes_tm_vhc_vehicle_nj_hb_his_df = DBUtils.readFromDB(spark, "smcvogg", "NJ_IMES_OWAPUSR01.TM_VHC_VEHICLE").get
    mes_tm_vhc_vehicle_nj_hb_his_df.createOrReplaceTempView(mes_tm_vhc_vehicle_nj_his)
    val mes_tm_vhc_vehicle_movement_nj_his = "mes_tm_vhc_vehicle_movement_nj_his"
    val mes_tm_vhc_vehicle_movement_nj_hb_his_df = DBUtils.readFromDB(spark, "smcvogg", "NJ_IMES_OWAPUSR01.TM_VHC_VEHICLE_MOVEMENT").get
    mes_tm_vhc_vehicle_movement_nj_hb_his_df.createOrReplaceTempView(mes_tm_vhc_vehicle_movement_nj_his)


    // 执行中间表
    val mes_map_mid_nj = SQLUtils.getSQLString("mes_map_mid_nj").get
      .format(pt_1_day_ago, pt_1_day_ago, mes_tm_ofm_order_nj_his, mes_tm_vhc_vehicle_nj_his)
    println("excuting sql:")
    println(mes_map_mid_nj)
    val mes_mid_df = spark.sql(mes_map_mid_nj)
    val mes_vehicle_point_location_mes_nj = "mes_vehicle_point_location_mes_nj"
    mes_mid_df.createOrReplaceTempView(mes_vehicle_point_location_mes_nj)

    //执行结果表
    val mes_map_ret_nj = SQLUtils.getSQLString("mes_map_ret_nj").get
      .format(mes_tm_vhc_vehicle_movement_nj_his, mes_vehicle_point_location_mes_nj,
        data_date_90_day_ago, data_date, data_date)
    println("excuting sql:")
    println(mes_map_ret_nj)

    val mes_rst_df = spark.sql(mes_map_ret_nj)


    //write to mysql
    //DBUtils.writeToDB(spark, mes_rst_df, "sqdt", "mes_vehicle_point_location_visual_map_nj_ft_5min", SaveMode.Append)

    spark.stop()
  }


}


