package cn.itcast.util

import org.apache.spark.sql.SparkSession

/**
 * @program: insurance_jb49
 * @description
 * @author: chenjia868
 * @create: 2021-05-06 10:59
 * */
object IteratorC4For {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]")
      .appName("ImportData")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")


    spark.sql("""cache table mytab0 as
                |select 1 as c1 , 1 as c2 , 6 as c3 union all
                |select 1  , 2  , 23  union all
                |select 1  , 3  , 8  union all
                |select 1  , 4  , 4  union all
                |select 2  , 1  , 32  union all
                |select 2  , 2  , 9  union all
                |select 2  , 3  , 15  union all
                |select 2  , 4  , 8 """.stripMargin)

    spark.sql("""
                |select c1,c2,c3,
                |       case when c2=1 then 1d
                |           else null
                |       end    as c4
                |from mytab0""".stripMargin).createOrReplaceTempView("mytab_temp")
    for(i <-2 to 4){
      spark.sql(s"""
                  |select c1,c2,c3,
                  |       case when c2=${i} then
                  |           (lag(c4) over (partition by c1 order by c2)+c3)/2
                  |            else c4
                  |           end    as c4
                  |from mytab_temp""".stripMargin).createOrReplaceTempView("mytab_temp")
    }
    spark.sql("select * from mytab_temp").show()
    spark.close()

  }
}
