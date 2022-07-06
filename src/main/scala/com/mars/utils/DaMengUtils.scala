package com.mars.utils

import com.mars.constant.Constant
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties

/**
 * DaMengUtils 达梦数据库工具类
 *
 * @author MarsHan
 * @Date 2022/6/09
 */
object DaMengUtils {
  var DM_PROP: Properties = new Properties()
  DM_PROP.setProperty("user", Constant.DM_USER_NAME)
  DM_PROP.setProperty("password", Constant.DM_PASS_WARD)
  DM_PROP.setProperty("driver", Constant.DM_JDBC_DRIVER)

  /**
   * 从达梦数据库中读取数据
   *
   * @param spark SparkSession
   * @param table 读取的表名
   * @return DataFrame
   */
  def DM2DF(spark: SparkSession, table: String): DataFrame = {
    spark.read.jdbc(Constant.DM_URL, Constant.DM_SCHEMA + "." + table, DM_PROP)
  }

  /**
   * 向达梦数据库写入数据
   *
   * @param df    DataFrame
   * @param table 写入的表名
   */
  def DF2DM(df: DataFrame, table: String): Unit = {
    df.write.mode(SaveMode.Overwrite).jdbc(Constant.DM_URL, Constant.DM_SCHEMA + "." + table, DM_PROP)
  }

}
