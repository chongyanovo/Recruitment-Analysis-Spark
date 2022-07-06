package com.mars.core


import com.mars.constant.Constant
import com.mars.utils.DaMengUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

/**
 * 多数据源配置
 *
 * @author MarsHan
 * @Date 2022/6/29
 */
object DataSource {
  var prop = new Properties()


  // id 数据访问地址 数据文件名(表名) 账号  密码  数据驱动类型  数据源类型(1 MySQL数据库,2 达梦数据库,3 HDFS 文件,4 Hive 表,...,5 csv ) 文件类型 是否启用
  // id data_url   table_name    username  password  driver  data_type file_type status

  /**
   * 获取配置的多数据源数据
   *
   * @param spark SparkSession
   * @return DataFrame
   */
  def getData(spark: SparkSession): DataFrame = {
    val dataSourceArray: Array[(String, String, String, String, String, String, String)] = getDataSourceArray(spark)
    var df: DataFrame = null
    for (dataSource <- dataSourceArray) {
      val dataUrl: String = dataSource._1
      val tableName: String = dataSource._2
      val userName: String = dataSource._3
      val passWord: String = dataSource._4
      val driver: String = dataSource._5
      val dataType: String = dataSource._6
      val fileType: String = dataSource._7

      if (isFileTypeTable(fileType)) { // 是表
        if (dataType == Constant.MYSQL_TYPE) {
          if (df == null) {
            df = getMySQLSource(spark, dataUrl, tableName, userName, passWord, driver)
          } else {
            df = getMySQLSource(spark, dataUrl, tableName, userName, passWord, driver).union(df)
          }

        } else if (dataType == Constant.DA_TYPE) {
          if (df == null) {
            df = getDAMENGSource(spark, dataUrl, tableName, userName, passWord, driver)
          } else {
            df = getDAMENGSource(spark, dataUrl, tableName, userName, passWord, driver).union(df)
          }
        } else if (dataType == Constant.OTHERS_TYPE) {
          if (df == null) {
            df = getOthersSource(spark, dataUrl, tableName, userName, passWord, driver)
          } else {
            df = getOthersSource(spark, dataUrl, tableName, userName, passWord, driver).union(df)
          }
        }
      } else { // 不是表
        if (df == null) {
          df = getHDFSSource(spark, dataUrl, tableName, fileType)
        } else {
          df = getHDFSSource(spark, dataUrl, tableName, fileType).union(df)
        }
      }
    }
    df
  }

  /**
   * 获取 MySQL 数据源数据
   *
   * @param spark     SparkSession
   * @param url       访问地址
   * @param tableName 读取的表名
   * @param userName  数据库用户名
   * @param passWord  数据库密码
   * @param driver    数据库驱动名
   * @return DataFrame
   */
  def getMySQLSource(spark: SparkSession, url: String, tableName: String, userName: String, passWord: String, driver: String): DataFrame = {
    setProperty(userName, passWord, driver)
    spark.read.jdbc(url = url, table = tableName, properties = prop)
  }

  /**
   * 获取 达梦 数据源数据
   *
   * @param spark     SparkSession
   * @param url       访问地址
   * @param tableName 读取的表名
   * @param userName  数据库用户名
   * @param passWord  数据库密码
   * @param driver    数据库驱动名
   * @return DataFrame
   */
  def getDAMENGSource(spark: SparkSession, url: String, tableName: String, userName: String, passWord: String, driver: String): DataFrame = {
    setProperty(userName, passWord, driver)
    spark.read.jdbc(url = url, table = tableName, properties = prop)
  }

  /**
   * 获取 HDFS 数据源数据
   *
   * @param spark    SparkSession
   * @param url      访问地址
   * @param fileName 读取的文件名
   * @return DataFrame
   */
  def getHDFSSource(spark: SparkSession, url: String, fileName: String, fileType: String): DataFrame = {
    val path: String = url + fileName
    spark.read.format(fileType).option("header", "true").load(path)
  }

  /**
   * 获取 Hive 数据源数据
   *
   * @param spark     SparkSession
   * @param url       访问地址
   * @param tableName 读取的表名
   * @param userName  数据库用户名
   * @param passWord  数据库密码
   * @param driver    数据库驱动名
   * @return DataFrame
   */
  def getHiveSource(spark: SparkSession, url: String, tableName: String, userName: String, passWord: String, driver: String): Unit = {
    setProperty(userName, passWord, driver)
    spark.sql(s"use ${tableName}")
  }

  /**
   * 获取 其他 数据源数据
   *
   * @param spark     SparkSession
   * @param url       访问地址
   * @param tableName 读取的表名
   * @param userName  数据库用户名
   * @param passWord  数据库密码
   * @param driver    数据库驱动名
   * @return DataFrame
   */
  def getOthersSource(spark: SparkSession, url: String, tableName: String, userName: String, passWord: String, driver: String): DataFrame = {
    setProperty(userName, passWord, driver)
    spark.read.jdbc(url = url, table = tableName, properties = prop)
  }

  /**
   * 设置数据库连接配置文件
   *
   * @param userName 数据库用户名
   * @param passWord 数据库密码
   * @param driver   数据库驱动名
   */
  def setProperty(userName: String, passWord: String, driver: String): Unit = {
    prop.setProperty("user", userName)
    prop.setProperty("password", passWord)
    prop.setProperty("driver", driver)
  }

  /**
   * 判断数据源是否为表
   *
   * @param dataType 数据源类型
   * @return flag Boolean 是否为表
   */
  def isFileTypeTable(fileType: String): Boolean = {
    var flag: Boolean = false
    if (fileType == Constant.TABLE_TYPE) {
      flag = true
    }
    flag
  }

  /**
   * 从数据库中读取数据源
   *
   * @param spark SparkSession
   * @return dataSourceArray Array[(String, String, String, String, String, String, String, String)
   */
  def getDataSourceArray(spark: SparkSession): Array[(String, String, String, String, String, String, String)] = {
    val dataSource: DataFrame = DaMengUtils.DM2DF(spark, Constant.DATA_SOURCE)
    val dataSourceArray: Array[(String, String, String, String, String, String, String)] = dataSource.rdd.collect().map(line => {
      val dataUrl: String = line(1).toString
      val tableName: String = line(2).toString
      val userName: String = line(3).toString
      val passWord: String = line(4).toString
      val driver: String = line(5).toString
      val dataType: String = line(6).toString
      val fileType: String = line(7).toString
      val status: String = line(8).toString
      if (status == Constant.STATUS_ENABLE) {
        (dataUrl, tableName, userName, passWord, driver, dataType, fileType)
      } else {
        null
      }
    })
    dataSourceArray.filter(_ != null)
  }
}
