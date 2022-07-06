package com.mars.utils

import com.mars.constant.Constant
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties

/**
 * MySQLUtils 工具类
 *
 * @author MarsHan
 * @Date 2022/5/30
 */
object MySQLUtils {
  val LOCAL_URL = Constant.MySQL_URL

  val LOCAL_PROP = new Properties()
  LOCAL_PROP.setProperty("user", Constant.MySQL_USER_NAME) // 用户名
  LOCAL_PROP.setProperty("password", Constant.MySQL_PASS_WARD) // 密码
  LOCAL_PROP.setProperty("driver", Constant.MySQL_JDBC_DRIVER)

  /**
   * 从MySQL数据库中读取数据
   *
   * @param spark SparkSession
   * @param table 读取的表名
   * @return DataFrame
   */
  def MySQL2DF(spark: SparkSession, table: String): DataFrame = {
    spark.read.jdbc(url = LOCAL_URL, table = table, properties = LOCAL_PROP)
  }

  /**
   * 向MySQL数据库写入数据
   *
   * @param df DataFrame
   * @param table 写入的表名
   */
  def DF2MySQL(df: DataFrame, table: String): Unit = {
    df.write.mode(SaveMode.Overwrite).jdbc(url = LOCAL_URL, table = table, LOCAL_PROP)
  }
}
