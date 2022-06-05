package com.mars.core

import org.apache.spark.sql.DataFrame

/**
 * DataFrame 自定义处理方法
 *
 * @author MarsHan
 * @Date 2022/5/30
 */
object DataFrameHandle {

  /**
   *
   * @param df    DataFrame
   * @param field 待去空的字段名
   * @return
   */
  def NullFilter(df: DataFrame, field: String): DataFrame = {
    df.where(s"$field != '' ")
  }

}
