package com.mars.utils

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties

/**
 * DaMengUtils 达梦数据库工具类
 *
 * @author MarsHan
 * @Date 2022/6/09
 */
object DaMengUtils {
  val IP: String = "www.jhon.top"
  val SPACENAME_DATA: String = "RECRUITMENT_ANALYSIS_DATA"
  val SPACENAME: String = "RECRUITMENT_ANALYSIS"

  val JDBC_URL_DATA: String = s"jdbc:dm://$IP:5236/$SPACENAME_DATA?autoReconnect=true&useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true"
  val JDBC_URL: String = s"jdbc:dm://$IP:5236/$SPACENAME?autoReconnect=true&useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true"

  val DM_JDBC_DRIVER: String = "dm.jdbc.driver.DmDriver"

  val USER: String = "SYSDBA"
  val PASSWORD: String = "SYSDBA"

  val DMPROP: Properties = new Properties()
  DMPROP.setProperty("user", USER)
  DMPROP.setProperty("password", PASSWORD)
  DMPROP.setProperty("driver", DM_JDBC_DRIVER)

  /**
   * 从达梦数据库中读取数据
   *
   * @param spark SparkSession
   * @param table 读取的表名
   * @return DataFrame
   */
  def DM2DF(spark: SparkSession, table: String): DataFrame = {
    spark.read.jdbc(JDBC_URL_DATA, SPACENAME_DATA + "." + table, DMPROP)
  }

  /**
   * 向达梦数据库写入数据
   *
   * @param df    DataFrame
   * @param table 写入的表名
   */
  def DF2DM(df: DataFrame, table: String): Unit = {
    df.write.mode(SaveMode.Overwrite).jdbc(JDBC_URL, SPACENAME + "." + table, DMPROP)
  }

}
