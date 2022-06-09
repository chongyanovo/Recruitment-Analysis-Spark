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
    jobOnlineDF.select("TITLE")
      .where(StringHandle.getKeySQL("TITLE"))
      .createTempView("POSITION_COUNT")
    val sql =
      s"""select POSITION_RE(TITLE) POSITION, sum(1) POSITION_COUNT
         |from POSITION_COUNT
         |group by POSITION""".stripMargin
    DataFrameHandle.NullFilter(spark.sql(sql), "POSITION")
  }


  /**
   * 查询岗位的最高工资和最低工资信息
   *
   * @param spark       SparkSession
   * @param jobOnlineDF 初始 df
   * @return DataFrame
   */
  def getPayMinMax(spark: SparkSession, jobOnlineDF: DataFrame): DataFrame = {
    jobOnlineDF.select("TITLE", "PAY")
      .where(StringHandle.getKeySQL("TITLE"))
      .where("PAY like '%K-%K'")
      .createTempView("PAY_MIN_MAX_TMP")

    val sql =
      s"""select POSITION_RE(TITLE) POSITION, min(PAY_MIN_INT(PAY)) PAY_MIN, max(PAY_MAX_INT(PAY)) PAY_MAX
         |from PAY_MIN_MAX_TMP
         |group by POSITION
         |order by POSITION desc""".stripMargin
    DataFrameHandle.NullFilter(spark.sql(sql), "POSITION")
  }

  /**
   * 统计每个城市招聘岗位的数量 position city key
   *
   * @param spark SparkSession
   * @return DataFrame
   */
  def getPositionCityKey(spark: SparkSession, yingJieShengDF: DataFrame): DataFrame = {
    yingJieShengDF.select("TITLE", "MSG")
      .where(StringHandle.getKeySQL("MSG"))
      .where(StringHandle.getKeySQL("TITLE"))
      .createTempView("YINGJIESHENG")

    val sql =
      s"""select GET_POSITION_MSG(MSG) POSITION,
         |GET_CITY_MSG(MSG) city,
         |GET_KEY_MSG(MSG) key
         |from YINGJIESHENG
         |""".stripMargin
    val df: DataFrame = DataFrameHandle.NullFilter(spark.sql(sql), "POSITION")
    df
  }


}
