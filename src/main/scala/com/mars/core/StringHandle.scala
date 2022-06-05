package com.mars.core

import com.mars.constant.Constant

/**
 * 字符串自定义处理方法
 * @author MarsHan
 * @Date 2022/5/30
 */
object StringHandle {

  /**
   * 获取查询字符串
   *
   * @param field 字段
   * @return keySQL
   */
  def getKeySQL(field: String): String = {
    var keySQL = ""
    Constant.KEYSLIST.map(key => {
      keySQL += s"$field like '%$key%' or "
    })
    keySQL.substring(0, keySQL.length - 3)
  }

  /**
   * 职位字符串处理
   * @param positionValue 职位所在字段名称
   * @return
   */
  def positionReplace(positionValue: String): String = {
    var replacedValue: String = positionValue;
    if (positionValue.contains("开发") | positionValue.contains("研发")) {
      replacedValue = "大数据开发工程师"
    }
    if (positionValue.contains("ETL") | positionValue.contains("etl")) {
      replacedValue = "大数据ETL工程师"
    }
    if (positionValue.contains("AI") | positionValue.contains("算法")) {
      replacedValue = "大数据算法工程师"
    }
    if (positionValue.contains("实习生")) {
      replacedValue = "大数据实习生"
    }
    if (positionValue.contains("挖掘")) {
      replacedValue = "大数据挖掘工程师"
    }
    if (positionValue.contains("分析")) {
      replacedValue = "大数据分析工程师"
    }
    if (positionValue.contains("架构")) {
      replacedValue = "大数据架构师"
    }
    if (positionValue.contains("运维")) {
      replacedValue = "大数据运维工程师"
    }
    if (positionValue.contains("测试")) {
      replacedValue = "大数据测试工程师"
    }
    if (positionValue.contains("经理")) {
      replacedValue = "大数据产品经理"
    }
    if (positionValue.contains("总监")) {
      replacedValue = "大数据总监"
    }
    if (positionValue.contains("运营")) {
      replacedValue = "大数据运营"
    }
    if (positionValue.contains("客服")) {
      replacedValue = "大数据客服"
    }
    if (positionValue.contains("安全")) {
      replacedValue = "大数据安全"
    }
    if (positionValue.contains("讲师")) {
      replacedValue = "大数据讲师"
    }
    if (positionValue.contains("售")) {
      replacedValue = "大数据销售业务"
    }
    if (positionValue.contains("风险")) {
      replacedValue = "大数据风险分析师"
    }
    if (positionValue.contains("招聘")) {
      replacedValue = ""
    }
    replacedValue
  }


}
