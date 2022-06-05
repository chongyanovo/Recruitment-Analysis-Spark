package com.mars.core

import com.mars.utils.MySQLUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Spark 相关方法
 *
 * @author MarsHan
 * @Date 2022/5/30
 */
object SparkCore {
  /**
   * 创建 SparkSession
   *
   * @param appName 任务名称
   * @param master  任务提交地址
   * @return SparkSession
   */
  def create(appName: String, master: String): SparkSession = {
    // 创建 SparkConf 配置文件
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    // 创建一个 SparkSession 类型的 spark 对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    spark
  }

  /**
   * 筛选出有关大数据的岗位
   *
   * @param spark       SparkSession
   * @param jobOnlineDF 初始 df
   * @return DataFrame
   */
  def getPositionCount(spark: SparkSession, jobOnlineDF: DataFrame): DataFrame = {
    jobOnlineDF.select("title")
      .where(StringHandle.getKeySQL("title"))
      .createTempView("position_count")
    val sql =
      s"""select position_re(title) position, sum(1) position_count
         |from position_count
         |group by position""".stripMargin
    DataFrameHandle.NullFilter(spark.sql(sql), "position")
  }


  /**
   * 查询岗位的最高工资和最低工资信息
   *
   * @param spark       SparkSession
   * @param jobOnlineDF 初始 df
   * @return DataFrame
   */
  def getPayMinMax(spark: SparkSession, jobOnlineDF: DataFrame): DataFrame = {
    jobOnlineDF.select("title", "pay")
      .where(StringHandle.getKeySQL("title"))
      .where("pay like '%K-%K'")
      .createTempView("pay_min_max_tmp")

    val sql =
      s"""select position_re(title) position, min(pay_min_int(pay)) pay_min, max(pay_max_int(pay)) pay_max
         |from pay_min_max_tmp
         |group by position
         |order by position desc""".stripMargin
    DataFrameHandle.NullFilter(spark.sql(sql), "position")
  }

  /**
   * 统计每个城市招聘岗位的数量 position city key
   *
   * @param spark SparkSession
   * @return DataFrame
   */
  def getPositionCityKey(spark: SparkSession): DataFrame = {
    var yingJieShengDF: DataFrame = MySQLUtils.MySQL2DF(spark, "yingjiesheng")
    yingJieShengDF = yingJieShengDF.select("title", "msg")
      .where(StringHandle.getKeySQL("msg"))
      .where(StringHandle.getKeySQL("title"))
    yingJieShengDF.createTempView("yingjiesheng")
    val sql =
      s"""select get_position_msg(msg) position,
         |get_city_msg(msg) city,
         |get_key_msg(msg) key
         |from yingjiesheng
         |""".stripMargin
    val df: DataFrame = DataFrameHandle.NullFilter(spark.sql(sql), "position")
    df
  }


}
