package com.mars

import com.mars.constant.Constant
import com.mars.core.{SparkCore, UdfRegisterHandle}
import com.mars.utils.{DaMengUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}


object RecruitmentAnalysisSparkAPP {
  def main(args: Array[String]): Unit = {
    // 获取 SparkSession 对象
    val spark: SparkSession = SparkCore.create("RecruitmentAnalysisSparkAPP", Constant.LOCALHOST)
    // 统一注册所有 UDF 函数
    UdfRegisterHandle.registerAll(spark)

    // 读取 JOB_ONLINE 表并转为 DataFrame
    val jobOnlineDF: DataFrame = DaMengUtils.DM2DF(spark, "JOB_ONLINE")

    // 筛选出有关大数据的岗位
    val positionCountDF: DataFrame = SparkCore.getPositionCount(spark, jobOnlineDF)
    positionCountDF.show()

    // 查询岗位的最高工资和最低工资信息
    val PayMinMaxDF: DataFrame = SparkCore.getPayMinMax(spark, jobOnlineDF)
    PayMinMaxDF.show()


    // 读取 YINGJIESHENG 表并转为 DataFrame
    val yingJieShengDF: DataFrame = DaMengUtils.DM2DF(spark, "YINGJIESHENG")

    // 统计每个城市招聘岗位的数量 position city key
    val PositionCityKeyDF: DataFrame = SparkCore.getPositionCityKey(spark, yingJieShengDF)
    PositionCityKeyDF.show()

    // 写入数据库
    //    DaMengUtils.DF2DM(positionCountDF,"POSITION_COUNT")
    //    println("POSITION_COUNT 存储成功")
    //    DaMengUtils.DF2DM(PayMinMaxDF,"PAY_MIN_MAX")
    //    println("PAY_MIN_MAX 存储成功")
    //    DaMengUtils.DF2DM(PositionCityKeyDF,"POSITION_CITY_KEY")
    //    println("POSITION_CITY_KEY 存储成功")


    spark.stop()
  }


}
