package com.mars.core

import com.mars.constant.Constant
import com.mars.utils.{DaMengUtils, MySQLUtils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.monotonically_increasing_id
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
    val conf: SparkConf = new SparkConf().setMaster(master).setAppName(appName)
    // 创建一个 SparkSession 类型的 spark 对象
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    // 给Constant大数据相关的关键词字典赋值
    setKeyWordsMap2Constant(spark)
    // 给Constant大数据相关的岗位字典赋值
    setPositionValueMap2Constant(spark)
    spark
  }

  /**
   * 获取清洗抽取完成的数据表
   *
   * @param spark SparkSession
   * @return DataFrame 清洗抽取完成的数据表
   */
  def getETL(spark: SparkSession): DataFrame = {
    // 公司类型 公司规模 学历要求 薪资平均值
    val baseDataDF: DataFrame = DataSource.getData(spark).distinct()
      .select("title", "type", "pay_text", "address",
        "experience", "education_background", "keywords", "release_time", "pay_min", "pay_max",
        "company_name", "company_type","company_scale","education_background",
        "address_detail", "treatment")
      .where("title != ''")
      .where("keywords != '' ")
    baseDataDF.createTempView("BASE_DATA")
    println("job_standard 数据 去重后:" + baseDataDF.count())
    val sql: String =
      s"""select title2position(title) POSITION,
         |time2year(release_time) YEAR,
         |time2month(release_time) MONTH,
         |time2day(release_time) DAY,
         |address2city(address) CITY,
         |keywordsFilter(keywords) KEYWORDS,
         |pay_min SALARY_MIN,
         |pay_max SALARY_MAX,
         |salary2avg(pay_min,pay_max) SALARY_AVG,
         |salary2salarySection(pay_min,pay_max)  SALARY_SECTION,
         |experience2experienceSection(experience) EXPERIENCE_SECTION,
         |company_name COMPANY_NAME,
         |company_type COMPANY_TYPE,
         |company_scale COMPANY_SCALE,
         |education_background EDUCATION_BACKGROUND,
         |address_detail ADDRESS_DETAIL,
         |treatment TREATMENT
         |from BASE_DATA """.stripMargin
    val ETL: DataFrame = spark.sql(sql)
      .where("POSITION != 'other' ")
    println("ETL 数据 :" + ETL.count())
    ETL.cache()
    ETL
  }

  /**
   * 获取数据大屏需要的数据表
   *
   * @param spark SparkSession
   * @param ETL   清洗过后的数据
   * @return DataFrame 数据大屏需要的数据表
   */
  def getVData(spark: SparkSession, ETL: DataFrame): DataFrame = {
    val dataDF: DataFrame = ETL.drop("EXPERIENCE", "COMPANY_NAME", "ADDRESS_DETAIL",
      "COMPANY_TYPE","COMPANY_SCALE","EDUCATION_BACKGROUND","SALARY_AVG",
      "TREATMENT", "SALARY_SECTION", "EXPERIENCE_SECTION")
    dataDF
  }


  /**
   * 获取 岗位关键字相关信息表
   *
   * @param spark SparkSession
   * @param ETL   清洗过后的数据
   * @return DataFrame 岗位关键字相关信息表
   */
  def getKeyWordsCountRelevant(spark: SparkSession, ETL: DataFrame): DataFrame = {
    import spark.implicits._
    val KeyWordsTuplesArray: Array[(String, String, String, String, String, String)] = ETL.select("POSITION", "KEYWORDS", "YEAR", "MONTH", "CITY", "SALARY_SECTION")
      .rdd.collect()
      .map(line => {
        val position: String = line(0).toString
        val keyword: String = line(1).toString
        val year: String = line(2).toString
        val month: String = line(3).toString
        val city: String = line(4).toString
        val salarySection: String = line(5).toString
        (position, keyword, year, month, city, salarySection)
      })

    val KeyWordsCountRelevantDF: DataFrame = spark.sparkContext.parallelize(KeyWordsTuplesArray)
      .map(line => {
        var Tuple: ((String, String, String, String, String, String), Int) = (("", "", "", "", "", ""), 1)
        val position: String = line._1
        val keywords: Array[String] = line._2.split("\\|")
        for (key <- keywords) {
          Tuple = ((position, key, line._3, line._4, line._5, line._6), 1)
        }
        Tuple
      })
      .reduceByKey(_ + _)
      .filter(_._1._2 != "")
      .map(tuple => {
        (tuple._1._1, tuple._1._3, tuple._1._4, tuple._1._5, tuple._1._6, tuple._1._2, tuple._2)
      })
      .toDF("POSITION", "YEAR", "MONTH", "CITY", "SALARY_SECTION", "KEYWORD", "KEYWORD_COUNT")

    KeyWordsCountRelevantDF
  }


  /**
   * 获取数据库中的大数据相关的关键词数据并转为关键字元组
   *
   * @param spark SparkSession
   * @return
   */
  def getKeyWordsMap(spark: SparkSession): Map[String, String] = {
    var keyMap: Map[String, String] = Map()
    val keyWordsDF: DataFrame = DaMengUtils.DM2DF(spark, Constant.KEYWORDS_TABLE)
    val keyWordsMapsArray: Array[(String, String, String)] = keyWordsDF.rdd.collect().map(line => {
      val keyValue: String = line(1).toString
      val replaceKeyValue: String = line(2).toString
      val status: String = line(3).toString
      (keyValue, replaceKeyValue, status)
    })
    for (key <- keyWordsMapsArray.filter(_._3 == Constant.STATUS_ENABLE)) {
      keyMap += key._1 -> key._2
    }
    keyMap
  }

  /**
   * 给Constant大数据相关的关键词字典赋值(只执行一次)
   *
   * @param spark SparkSession
   * @return
   */
  def setKeyWordsMap2Constant(spark: SparkSession): Unit = {
    Constant.keyWordsMap = getKeyWordsMap(spark)
  }

  /**
   * 获取数据库中的大数据相关的岗位数据并转为关键字元组
   *
   * @param spark SparkSession
   * @return
   */
  def getPositionValueMap(spark: SparkSession): Map[String, String] = {
    var positionValueMap: Map[String, String] = Map()
    val positionValueDF: DataFrame = MySQLUtils.MySQL2DF(spark, Constant.POSITION_TABLE)
    val positionValueMapsArray: Array[(String, String, String)] = positionValueDF.rdd.collect().map(line => {
      val positionValue: String = line(1).toString
      val replacePositionValue: String = line(2).toString
      val status: String = line(3).toString
      (positionValue, replacePositionValue, status)
    })
    for (position <- positionValueMapsArray.filter(_._3 == Constant.STATUS_ENABLE)) {
      positionValueMap += position._1 -> position._2
    }
    positionValueMap
  }

  /**
   * 给Constant大数据职位名称字典赋值(只执行一次)
   *
   * @param spark SparkSession
   * @return
   */
  def setPositionValueMap2Constant(spark: SparkSession): Unit = {
    Constant.positionValueMap = getPositionValueMap(spark)
  }
}
