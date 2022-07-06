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
  var keyWordsMap: Map[String, String] = null

  // 关键字表配置 KEYWORDS
  val KEYWORDS_TABLE:String = "KEYWORDS"

  // 大数据相关的岗位
  var positionValueMap: Map[String, String] = null

  // 关键字表配置 POSITION_VALUE
  val POSITION_TABLE:String = "POSITION_VALUE"

  // 多数据源配置
  val DATA_SOURCE:String = "DATA_SOURCE"

  val MYSQL_TYPE:String = "mysql"
  val DA_TYPE:String = "dameng"
  val HIVE_TYPE:String = "hive"
  val HDFS_TYPE:String = "hdfs"
  val OTHERS_TYPE:String = "other"
  val STATUS_ENABLE:String = "1"
  val STATUS_DISABLE:String = "0"
  val TABLE_TYPE:String = "table"



  // 达梦数据库配置
  val DM_URL:String = "jdbc:dm://119.96.188.91:30980/RECRUITMENT_ANALYSIS?autoReconnect=true&useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true"
  val DM_SCHEMA:String = "RECRUITMENT_ANALYSIS"
  val DM_USER_NAME:String = "SYSDBA"
  val DM_PASS_WARD:String = "SYSDBA"
  val DM_JDBC_DRIVER:String = "dm.jdbc.driver.DmDriver"

  // MySQL数据库配置
  val MySQL_URL:String = "jdbc:mysql://localhost:3306/recruitment_analysis"
  val MySQL_SCHEMA:String = "recruitment_analysis"
  val MySQL_USER_NAME:String = "root"
  val MySQL_PASS_WARD:String = "root"
  val MySQL_JDBC_DRIVER:String = "com.mysql.cj.jdbc.Driver"
}



// INSERT INTO `keywords` VALUES ("机器学习","机器学习","1");
// INSERT INTO `keywords` VALUES ("中台","中台","1");
// INSERT INTO `keywords` VALUES ("数据分析","数据分析","1");
// INSERT INTO `keywords` VALUES ("大数据","大数据","1");
// INSERT INTO `keywords` VALUES ("数据仓库","数据仓库","1");
// INSERT INTO `keywords` VALUES ("离线数仓","离线数仓","1");
// INSERT INTO `keywords` VALUES ("实时数仓","实时数仓","1");
// INSERT INTO `keywords` VALUES ("数据治理","数据治理","1");
// INSERT INTO `keywords` VALUES ("数据挖掘","数据挖掘","1");
// INSERT INTO `keywords` VALUES ("流计算","流计算","1");
// INSERT INTO `keywords` VALUES ("数据库","数据库","1");
// INSERT INTO `keywords` VALUES ("爬虫","爬虫","1");
// INSERT INTO `keywords` VALUES ("OLAP","OLAP","1");
// INSERT INTO `keywords` VALUES ("OLTP","OLTP","1");
// INSERT INTO `keywords` VALUES ("HiveSQL","HiveSQL","1");
// INSERT INTO `keywords` VALUES ("SQL","SQL","1");
// INSERT INTO `keywords` VALUES ("Docker","Docker","1");
// INSERT INTO `keywords` VALUES ("SparkSQL","SparkSQL","1");
// INSERT INTO `keywords` VALUES ("Hadoop","Hadoop","1");
// INSERT INTO `keywords` VALUES ("hadoop","Hadoop","1");
// INSERT INTO `keywords` VALUES ("HDFS","HDFS","1");
// INSERT INTO `keywords` VALUES ("hdfs","HDFS","1");
// INSERT INTO `keywords` VALUES ("Spark","Spark","1");
// INSERT INTO `keywords` VALUES ("spark","Spark","1");
// INSERT INTO `keywords` VALUES ("Flink","Flink","1");
// INSERT INTO `keywords` VALUES ("flink","Flink","1");
// INSERT INTO `keywords` VALUES ("Hive","Hive","1");
// INSERT INTO `keywords` VALUES ("hive","Hive","1");
// INSERT INTO `keywords` VALUES ("Flume","Flume","1");
// INSERT INTO `keywords` VALUES ("flume","Flume","1");
// INSERT INTO `keywords` VALUES ("Sqoop","Sqoop","1");
// INSERT INTO `keywords` VALUES ("sqoop","Sqoop","1");
// INSERT INTO `keywords` VALUES ("Storm","Storm","1");
// INSERT INTO `keywords` VALUES ("storm","Storm","1");
// INSERT INTO `keywords` VALUES ("BI","BI","1");
// INSERT INTO `keywords` VALUES ("bi","BI","1");
// INSERT INTO `keywords` VALUES ("Hbase","Hbase","1");
// INSERT INTO `keywords` VALUES ("hbase","Hbase","1");
// INSERT INTO `keywords` VALUES ("Presto","Presto","1");
// INSERT INTO `keywords` VALUES ("presto","Presto","1");
// INSERT INTO `keywords` VALUES ("Kafka","Kafka","1");
// INSERT INTO `keywords` VALUES ("kafka","Kafka","1");
// INSERT INTO `keywords` VALUES ("Zookeeper","Zookeeper","1");
// INSERT INTO `keywords` VALUES ("zookeeper","Zookeeper","1");
// INSERT INTO `keywords` VALUES ("Azkaban","Azkaban","1");
// INSERT INTO `keywords` VALUES ("azkaban","Azkaban","1");
// INSERT INTO `keywords` VALUES ("ETL","ETL","1");
// INSERT INTO `keywords` VALUES ("etl","ETL","1");
// INSERT INTO `keywords` VALUES ("Kettle","Kettle","1");
// INSERT INTO `keywords` VALUES ("kettle","Kettle","1");
// INSERT INTO `keywords` VALUES ("MySQL","MySQL","1");
// INSERT INTO `keywords` VALUES ("mysql","MySQL","1");
// INSERT INTO `keywords` VALUES ("Python","Python","1");
// INSERT INTO `keywords` VALUES ("python","Python","1");
// INSERT INTO `keywords` VALUES ("Java","Java","1");
// INSERT INTO `keywords` VALUES ("java","Java","1");
// INSERT INTO `keywords` VALUES ("Redis","Redis","1");
// INSERT INTO `keywords` VALUES ("redis","Redis","1");
// INSERT INTO `keywords` VALUES ("MapReduce","MapReduce","1");
// INSERT INTO `keywords` VALUES ("mapreduce","MapReduce","1");
// INSERT INTO `keywords` VALUES ("AI","AI","1");
// INSERT INTO `keywords` VALUES ("ai","AI","1");

// keyValue  replaceKeyValue  status
