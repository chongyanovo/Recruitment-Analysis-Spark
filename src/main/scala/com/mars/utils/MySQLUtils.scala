package com.mars.utils

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties

/**
 * MySQLUtils 工具类
 *
 * @author MarsHan
 * @Date 2022/5/30
 */
object MySQLUtils {
  val LOCALURL = "jdbc:mysql://localhost:3306/recruitment_analysis"
  val REMOTEURL = "jdbc:mysql://www.partitioner.cn:3306/recruitment_analysis"

  val LOACLPROP = new Properties()
  LOACLPROP.setProperty("user", "root") // 用户名
  LOACLPROP.setProperty("password", "root") // 密码
  LOACLPROP.setProperty("driver", "com.mysql.cj.jdbc.Driver")
  LOACLPROP.setProperty("numPartitions", "2")

  val REMOTEPROP = new Properties()
  REMOTEPROP.setProperty("user", "root") // 用户名
  REMOTEPROP.setProperty("password", "123456") // 密码
  REMOTEPROP.setProperty("driver", "com.mysql.cj.jdbc.Driver")
  REMOTEPROP.setProperty("numPartitions", "2")

  /**
   * 从MySQL数据库中读取数据
   *
   * @param spark SparkSession
   * @param table 读取的表名
   * @return DataFrame
   */
  def MySQL2DF(spark: SparkSession, table: String): DataFrame = {
    spark.read.jdbc(url = LOCALURL, table = table, properties = LOACLPROP)
  }

  /**
   * 向MySQL数据库写入数据
   *
   * @param spark SparkSession
   * @param table 写入的表名
   */
  def DF2MySQL(dataFrame: DataFrame, table: String): Unit = {
    dataFrame.write.mode(SaveMode.Overwrite).jdbc(url = REMOTEURL, table = table, REMOTEPROP)
  }
}
