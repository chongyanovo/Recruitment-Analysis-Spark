package com.mars

import com.mars.constant.Constant
import com.mars.core.{SparkCore, UdfRegisterHandle}
import com.mars.utils.{DaMengUtils, MySQLUtils}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession, functions}


object RecruitmentAnalysisSparkAPP {
  def main(args: Array[String]): Unit = {
    // 获取 SparkSession 对象
    val spark: SparkSession = SparkCore.create("RecruitmentAnalysisSparkAPP", Constant.LOCALHOST)
    // 统一注册所有 UDF 函数
    UdfRegisterHandle.registerAll(spark)

    //    DaMengUtils.DM2DF(spark, "ETL_DATA")
    val ETL: DataFrame = SparkCore.getETL(spark)
    ETL.show()
    //    DaMengUtils.DF2DM(ETL, "ETL_DATA")

    // 公司类型 公司规模 学历要求 薪资平均值

    //    import spark.implicits._
    //    val treatmentsArray: Array[String] = ETL.select("TREATMENT")
    //      .rdd.collect()
    //      .flatMap(_.toString().split("\\|"))
    //    val treatmentDF: DataFrame = spark.sparkContext.parallelize(treatmentsArray)
    //      .map(_.replace("[", "")
    //        .replace("]", ""))
    //      .toDF( "TREATMENT")
    //      .filter("TREATMENT != '' ")
    //      .groupBy("TREATMENT")
    //      .count()
    //      .withColumnRenamed("count","TREATMENT_COUNT")
    //      .orderBy(col("TREATMENT_COUNT").desc)
    //      .filter("TREATMENT_COUNT >= 1000")

    //    treatmentDF.show(50)
    //    println(treatmentDF.count())
    //    DaMengUtils.DF2DM(treatmentDF,"TREATMENT")
    //    MySQLUtils.DF2MySQL(treatmentDF,"TREATMENT")


    // 数据大屏需要的数据
    //    val dataDF: DataFrame = SparkCore.getVData(spark, ETL)
    //    dataDF.show()
    //    println("数据大屏数据量:" + dataDF.count())
    //    DaMengUtils.DF2DM(dataDF, "DATA")


    //    val KeyWordsCountRelevantDF: DataFrame = SparkCore.getKeyWordsCountRelevant(spark, ETL)
    //    KeyWordsCountRelevantDF.show()
    //    DaMengUtils.DF2DM(KeyWordsCountRelevantDF, "KEY_WORDS_COUNT_RELEVANT")


    //.withColumn("id", monotonically_increasing_id() + 1)
    //.select("id", "POSITION", "YEAR", "MONTH", "CITY", "SALARY_SECTION", "KEYWORD", "KEYWORD_COUNT")


    spark.stop()
  }


}
