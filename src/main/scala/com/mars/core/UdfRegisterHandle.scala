package com.mars.core

import com.mars.constant.Constant
import com.mars.constant.Constant.keyWordsMap
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
    title2positionUDF(spark)

    time2yearUDF(spark)
    time2monthUDF(spark)
    time2dayUDF(spark)

    address2cityUDF(spark)

    keywordsFilterUDF(spark)

    experience2experienceSectionUDF(spark)

    salary2salarySectionUDF(spark)
    salary2avgUDF(spark)
  }

  /**
   * 职位字段替换 UDF 函数
   *
   * @param spark SparkSession
   */
  def title2positionUDF(spark: SparkSession): Unit = {
    spark.udf.register("title2position", (title: String) => {
      var replacedValue: String = "other";
      for (position <- Constant.positionValueMap.keys) {
        if (title.contains(position)) {
          replacedValue = Constant.positionValueMap.get(position).get
        }
      }
      replacedValue
    })
  }

  /**
   * 时间字段获取年份 UDF 函数
   *
   * @param spark SparkSession
   */
  def time2yearUDF(spark: SparkSession): Unit = {
    spark.udf.register("time2year", (time: String) => {
      var outTime: String = ""
      if (isTimeOK(time)) {
        outTime = time.split("-")(0)
      }
      outTime
    })
  }

  /**
   * 时间字段获取月份 UDF 函数
   *
   * @param spark SparkSession
   */
  def time2monthUDF(spark: SparkSession): Unit = {
    spark.udf.register("time2month", (time: String) => {
      var outTime: String = ""
      if (isTimeOK(time)) {
        outTime = time.split("-")(1)
      }
      outTime
    })
  }

  /**
   * 时间字段获取日期 UDF 函数
   *
   * @param spark SparkSession
   */
  def time2dayUDF(spark: SparkSession): Unit = {
    spark.udf.register("time2day", (time: String) => {
      var outTime: String = ""
      if (isTimeOK(time)) {
        outTime = time.split("-")(2)
      }
      outTime
    })
  }

  /**
   * 地址字段获取城市 UDF 函数
   *
   * @param spark SparkSession
   */
  def address2cityUDF(spark: SparkSession): Unit = {
    spark.udf.register("address2city", (address: String) => {
      var outAddress: String = ""
      val add: String = address.replace("[", "")
        .replace("]", "")
        .replace("·", "-")
      val addArray: Array[String] = add.split("-")
      outAddress = addArray(0) + "市"
      if (outAddress.contains("省")) {
        outAddress = ""
      }
      outAddress
    })
  }

  /**
   * 关键字过滤 UDF 函数
   *
   * @param spark SparkSession
   */
  def keywordsFilterUDF(spark: SparkSession): Unit = {
    spark.udf.register("keywordsFilter", (keywords: String) => {
      val keysList: Array[String] = keywords.split("\\|")
      var outKeys: String = ""
      val ConstantKeyWordsList: Array[String] = keyWordsMap.keys.toArray
      val commonKeyList: Array[String] = keysList.intersect(ConstantKeyWordsList)
      for (key <- commonKeyList) {
        outKeys = (outKeys + keyWordsMap.get(key).get + "|")
      }
      // 去字符串最后一个 "|"
      if (outKeys.size != 0) {
        outKeys = outKeys.substring(0, outKeys.size - 1)
      }
      outKeys
    })
  }

  /**
   * 工作经验转为区间形式
   *
   * @param spark
   */
  def experience2experienceSectionUDF(spark: SparkSession): Unit = {
    spark.udf.register("experience2experienceSection", (experience: String) => {
      val experienceArray: Array[String] = experience.split("年")
      var outExperience: String = "不清楚"
      if (experienceArray.length == 2) {
        val experienceYearArray: Array[String] = experienceArray(0).split("-")
        if (experienceYearArray.length == 1) {
          val experienceYear: String = experienceArray(0)
          outExperience = judgeExperienceSection(experienceYear)
        } else {
          val flag: Boolean = ((judgeExperienceSection(experienceYearArray(0)) == "")
            || (judgeExperienceSection(experienceYearArray(1)) == ""))
          if (flag) {
            val minExperienceYear = experienceYearArray(0).toInt
            val maxExperienceYear = experienceYearArray(1).toInt
            outExperience = judgeExperienceSection(minExperienceYear.+(maxExperienceYear).*(0.5).toInt.toString)
          } else {
            outExperience = judgeExperienceSection(experienceYearArray(0))
          }
        }
      } else {
        outExperience = "无需经验"
      }
      outExperience
    })
  }

  /**
   * 招聘薪资转为区间形式
   *
   * @param spark SparkSession
   */
  def salary2salarySectionUDF(spark: SparkSession): Unit = {
    spark.udf.register("salary2salarySection", (minPay: String, maxPay: String) => {
      val minSalary: Double = minPay.toDouble
      val maxSalary: Double = maxPay.toDouble
      val avgSalary: Double = minSalary.+(maxSalary).*(0.5)
      val salaryWeight: Int = avgSalary.toInt / 5000
      salaryWeight.*(5000).toString + "-" + salaryWeight.*(5000).+(5000)
    })
  }

  /**
   * 招聘薪资平均值
   *
   * @param spark SparkSession
   */
  def salary2avgUDF(spark: SparkSession): Unit = {
    spark.udf.register("salary2avg", (minPay: String, maxPay: String) => {
      minPay.toDouble.+(maxPay.toDouble).*(0.5)
    })
  }

  /**
   * 判断时间是否合法
   *
   * @param time 时间
   * @return
   */
  def isTimeOK(time: String): Boolean = {
    var flag: Boolean = false
    if (time.split("-").length == 3) {
      flag = true
    }
    flag
  }

  /**
   * 判断工作经验所处区间
   *
   * @param experienceYearStr 工作经验时长(年)
   * @return
   */
  def judgeExperienceSection(experienceYearStr: String): String = {
    // 0-3 3-5 5-10 10+
    var outExperienceYear: String = ""
    val experienceYear: Int = experienceYearStr.toInt
    if (0 < experienceYear && experienceYear <= 3) {
      outExperienceYear = "0-3"
    } else if (3 < experienceYear && experienceYear <= 5) {
      outExperienceYear = "3-5"
    } else if (5 < experienceYear && experienceYear <= 10) {
      outExperienceYear = "5-10"
    } else if (10 < experienceYear) {
      outExperienceYear = "10+"
    }
    outExperienceYear
  }
}


