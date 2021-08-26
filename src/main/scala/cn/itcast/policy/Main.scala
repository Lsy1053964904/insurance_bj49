package cn.itcast.policy

import cn.itcast.util.SparkUtil.executeSQLFile
import cn.itcast.util.UDAFLx
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import java.util.Properties

/**
 * @program: insurance_jb49
 * @description
 * @author: chenjia868
 * @create: 2021-05-05 11:39
 * */
object Main {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().master("local[*]")
      .appName("calculate_prem")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    //调用工具类，执行SQL文件的所有内容
    spark.udf.register("UDAFLx",new UDAFLx())
    //executeSQLFile(spark, "2_prem.sql")
    //executeSQLFile(spark, "3_cv.sql")
    //executeSQLFile(spark, "4_reserve.sql")
    executeSQLFile(spark, "5_statistics.sql")

    val url = "jdbc:mysql://node3:3306/insurance?createDatabaseIfNotExist=true&serverTimezone=UTC&characterEncoding=utf8&useUnicode=true&useSSL=false"
    val prop = new Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","123456")

    //如果mysql没有对应的表名，那么会自动创建表。
    spark.sql("select * from insurance_app.policy_actuary")
      .write.mode(SaveMode.Overwrite).jdbc(url,"insurance.policy_actuary",prop)


    /*
    //方式一 用scala 的 for循环来实现
        spark.sql(
          """
            |select *,
            |       if(policy_year=1,1d,null) as lx --有效保单数
            |from prem_src3
            |""".stripMargin).createOrReplaceTempView("prem_src4")

        val df: DataFrame = spark.sql("select max(policy_year) as max_index from prem_src4")
        val driver_arr: Array[Row] = df.collect()
        val max_index: Int = driver_arr(0).getInt(0)

        for (i <- 2 to max_index) {
          println("当前是第"+i+"轮循环：")
          spark.sql(
            s"""
              |select sex,
              |       ppp,
              |       age_buy,
              |       Nursing_Age,
              |       interest_rate,
              |       sa,
              |       t_age,
              |       bpp,
              |       policy_year,
              |       age,
              |       ppp_,
              |       bpp_,
              |       qx,
              |       kx,
              |       qx_ci,
              |       qx_d,
              |       if(policy_year = ${i},
              |          lag(lx * (1 - qx)) over (partition by sex,ppp,age_buy order by policy_year),
              |          lx) as lx
              |from prem_src4
              |""".stripMargin).createOrReplaceTempView("prem_src4")
        }
        spark.sql(
          """
            |select *
            |from prem_src4
            |where sex = 'M'
            |  and ppp = 10
            |  and age_buy = 18
            |order by policy_year  """.stripMargin).show()
    */
    /* 方式二：
        //使用UDAF函数
        //注册UDAF函数
        spark.udf.register("UDAFLx",new UDAFLx())
        //使用UDAF函数
        spark.sql(
          """
            |select sex,
            |       ppp,
            |       age_buy,
            |       Nursing_Age,
            |       interest_rate,
            |       sa,
            |       t_age,
            |       bpp,
            |       policy_year,
            |       age,
            |       ppp_,
            |       bpp_,
            |       qx,
            |       kx,
            |       qx_ci,
            |       qx_d,
            |       --用UDAF函数来计算后续所有的lx字段
            |       UDAFLx(lx,qx) over (partition by sex,ppp,age_buy order by policy_year) as lx
            |from prem_src4
            |""".stripMargin)
        spark.sql(
          """
            |select *
            |from prem_src4_2
            |where sex = 'M'
            |  and ppp = 10
            |  and age_buy = 18
            |order by policy_year
            |""".stripMargin).show()*/

    //步骤5 计算dx_d、dx_ci、lx_d字段

    /*   for( i <- 2 to max_index){
         spark.sql(
           s"""
             |select p.*,
             |       lx_d*qx_d as dx_d,
             |       lx_d*qx_ci as dx_ci
             |from (select sex,
             |             ppp,
             |             age_buy,
             |             Nursing_Age,
             |             interest_rate,
             |             sa,
             |             t_age,
             |             bpp,
             |             policy_year,
             |             age,
             |             ppp_,
             |             bpp_,
             |             qx,
             |             kx,
             |             qx_ci,
             |             qx_d,
             |             lx,
             |             if(policy_year = ${i}, lag(lx_d - dx_d - dx_ci) over (partition by sex,ppp,age_buy order by policy_year),
             |                lx_d) as lx_d --健康人数
             |      from prem_src5) p
             |""".stripMargin).createOrReplaceTempView("prem_src5")
       }

       spark.sql(
         """
           |select *
           |from prem_src5
           |where sex = 'M'
           |  and ppp = 10
           |  and age_buy = 18
           |order by policy_year
           |""".stripMargin).show()*/

    spark.close()

  }

}
