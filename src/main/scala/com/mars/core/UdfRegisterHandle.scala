package com.mars.core

import com.mars.constant.Constant
import org.apache.spark.sql.SparkSession

/**
 * UDF 函数统一注册
 *
 * @author MarsHan
 * @Date 2022/5/30
 */
object UdfRegisterHandle {

  /**
   * 注册所有 UDF 函数
   *
   * @param spark SparkSession
   */
  def registerAll(spark: SparkSession): Unit = {
    positionReplaceUDF(spark)
    payMinUDF(spark)
    payMaxUDF(spark)
    getCityByTitleUDF(spark)
    getPositionByTitleUDF(spark)
    getCompanyByTitleUDF(spark)
    getTimeByInfoUDF(spark)
    getCityByMsgUDF(spark)
    getKeyByMsgUDF(spark)
    getPositionByMsgUDF(spark)
  }

  /**
   * 职位字段替换 UDF 函数
   *
   * @param spark SparkSession
   */
  def positionReplaceUDF(spark: SparkSession): Unit = {
    spark.udf.register("POSITION_RE", (title: String) => {
      StringHandle.positionReplace(title)
    })
  }

  /**
   * 分割最低工资 UDF 函数
   *
   * @param spark SparkSession
   */
  def payMinUDF(spark: SparkSession): Unit = {
    spark.udf.register("PAY_MIN_INT", (pay: String) => {
      var minPay: Double = 0
      if (pay.contains("-")) {
        minPay = pay.split("-")(0).replace("K", "").trim.toDouble.*(1000)
      }
      minPay.toInt
    })
  }

  /**
   * 分割最高工资 UDF 函数
   *
   * @param spark SparkSession
   */
  def payMaxUDF(spark: SparkSession): Unit = {
    spark.udf.register("PAY_MAX_INT", (pay: String) => {
      var maxPay: Double = 0
      maxPay = pay.split("-")(1).replace("K", "").trim.toDouble.*(1000)
      maxPay.toInt
    })
  }

  /**
   * 通过 title 获取城市 UDF 函数
   *
   * @param spark SparkSession
   */
  def getCityByTitleUDF(spark: SparkSession): Unit = {
    spark.udf.register("GET_CITY", (title: String) => {
      var city: String = ""
      if (title.contains("[") && title.contains("]")) {
        city = title.split(" ")(0).split("]")(0).replace("[", "")
      }
      city
    })
  }

  /**
   * 通过 title 获取职位 UDF 函数
   *
   * @param spark SparkSession
   */
  def getPositionByTitleUDF(spark: SparkSession): Unit = {
    spark.udf.register("GET_POSITION", (title: String) => {
      var position: String = ""
      if (title.split(" ").length == 2) {
        position = StringHandle.positionReplace(title.split(" ")(1))
      }
      position
    })
  }

  /**
   * 通过 title 获取公司 UDF 函数
   *
   * @param spark SparkSession
   */
  def getCompanyByTitleUDF(spark: SparkSession): Unit = {
    spark.udf.register("GET_COMPANY", (title: String) => {
      var company: String = ""
      if (title.contains("[") && title.contains("]")) {
        company = title.split(" ")(0).split("]")(1)
      }
      else {
        company = title.split(" ")(0)
      }
      company
    })
  }

  /**
   * 通过 info 获取招聘日期 UDF 函数
   *
   * @param spark SparkSession
   */
  def getTimeByInfoUDF(spark: SparkSession): Unit = {
    spark.udf.register("GET_TIME", (info: String) => {
      info.split("\n")(0)
    })
  }

  /**
   * 通过 msg 获取城市 UDF 函数
   *
   * @param spark SparkSession
   */
  def getCityByMsgUDF(spark: SparkSession): Unit = {
    spark.udf.register("GET_CITY_MSG", (info: String) => {
      var city: String = ""
      val line: String = info.split("\n")(3)
      val citys: String = line.split("\\s\\|\\s")(2).trim
      if (citys.contains(",")) {
        city = citys.split(",")(0)
      }
      else {
        city = citys
      }
      city
    })
  }

  /**
   * 通过 msg 获取关键字 UDF 函数
   *
   * @param spark SparkSession
   */
  def getKeyByMsgUDF(spark: SparkSession): Unit = {
    spark.udf.register("GET_KEY_MSG", (msg: String) => {
      val keyLine: String = msg.split("\n")(1).replace("职位简介：", "")
        .replace(".", "")
      var keyString: String = ""
      Constant.KEYSLIST.map(key => {
        if (keyLine.contains(key)) {
          keyString += key + " "
        }
      })
      keyString.trim
    })
  }

  /**
   * 通过 msg 获取职位 UDF 函数
   *
   * @param spark SparkSession
   */
  def getPositionByMsgUDF(spark: SparkSession): Unit = {
    spark.udf.register("GET_POSITION_MSG", (msg: String) => {
      val position: String = msg.split("\n")(0).split("\\s")(1)
      StringHandle.positionReplace(position)
    })
  }
}


