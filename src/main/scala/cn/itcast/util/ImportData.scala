package cn.itcast.util

import org.apache.spark.sql.{Row, SparkSession}

import java.util.Properties

/**
 * @program: insurance_jb49
 * @description
 * @author: chenjia868
 * @create: 2021-05-05 16:30
 * */
object ImportData {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]")
      .appName("ImportData")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val arr: Array[String] = Array("area",
      "claim_info",
      "cv_src",
      "dd_table",
      "mort_10_13",
      //"policy_acuary",
      "policy_benefit",
      "policy_client",
      "policy_surrender",
      "pre_add_exp_ratio",
      "prem_cv_real",
      "prem_std_real",
      "rsv_src")
    for(tablename <- arr){
      importData(spark,tablename)
    }
    spark.close()
  }

  def importData(spark:SparkSession,tablename:String)={
    //spark加载MySQL的表数据，生成一个DataFrame
    val url = "jdbc:mysql://node3:3306/insurance?createDatabaseIfNotExist=true&serverTimezone=UTC&characterEncoding=utf8&useUnicode=true&useSSL=false"
    val prop = new Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","123456")
    //将DataFrame写入到hive的表中。
    spark.read.jdbc(url,tablename,prop).write.format("hive").mode("overwrite").saveAsTable("insurance_ods."+tablename)
    //检测2边数据是否一致
    val mysql_cnt: Long = spark.read.jdbc(url, tablename, prop).count()
    val rows: Array[Row] = spark.sql("select count(1) from insurance_ods." + tablename).collect()
    val hive_cnt: Long = rows(0).getLong(0)
    if(mysql_cnt==hive_cnt){
      println("MySQL表的数据量有"+mysql_cnt+"条，hive表数据量有"+hive_cnt+"条，2边数据量一致")
    }else{
      println("MySQL表的数据量有"+mysql_cnt+"条，hive表数据量有"+hive_cnt+"条，2边数据量不一致")
    }
  }

}
