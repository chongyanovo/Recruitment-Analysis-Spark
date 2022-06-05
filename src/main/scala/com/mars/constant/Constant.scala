package com.mars.constant

/**
 * 常量类
 *
 * @author MarsHan
 * @Date 2022/5/30
 */
object Constant {
  // Spark 调用本地模式运行
  val LOCALHOST: String = "local[*]";

  // 大数据相关的关键词
  val KEYSLIST: List[String] = List("大数据", "数据仓库", "离线数仓", "实时数仓", "数据挖掘", "Hadoop", "hadoop", "HDFS", "hdfs",
    "Spark", "spark", "Flink", "flink", "Hive", "hive", "Flume", "flume", "Sqoop", "sqoop", "Storm", "storm", "BI", "bi", "Hbase", "hbase",
    "Presto", "presto", "Kafka", "kafka", "Zookeeper", "zookeeper", "Azkaban", "azkaban")
}
