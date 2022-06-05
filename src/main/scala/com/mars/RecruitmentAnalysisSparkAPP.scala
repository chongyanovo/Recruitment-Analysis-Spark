package com.mars

import com.mars.constant.Constant
import com.mars.core.{SparkCore, UdfRegisterHandle}
import com.mars.utils.MySQLUtils
import org.apache.spark.sql.{DataFrame, SparkSession}



object RecruitmentAnalysisSparkAPP {
  def main(args: Array[String]): Unit = {
    // 获取 SparkSession 对象
    val spark: SparkSession = SparkCore.create("APP", Constant.LOCALHOST)
    // 统一注册所有 UDF 函数
    UdfRegisterHandle.registerAll(spark)

    // 读取 job_online 表并转为 DataFrame
    val jobOnlineDF: DataFrame = MySQLUtils.MySQL2DF(spark, "job_online")

    // 筛选出有关大数据的岗位
    val positionCountDF: DataFrame = SparkCore.getPositionCount(spark, jobOnlineDF)
    positionCountDF.show()

    // 查询岗位的最高工资和最低工资信息
    val PayMinMaxDF: DataFrame = SparkCore.getPayMinMax(spark, jobOnlineDF)
    PayMinMaxDF.show()

    // 统计每个城市招聘岗位的数量 position city key
    val PositionCityKeyDF: DataFrame = SparkCore.getPositionCityKey(spark)
    PositionCityKeyDF.show()

    spark.stop()
  }


}
