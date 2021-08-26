package cn.itcast.util

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, StructField, StructType}

/**
 * @program: insurance_jb49
 * @description
 * @author: chenjia868
 * @create: 2021-05-06 11:32
 * */
object IteratorUDAF {

  //定义一个子类继承
  class UDAFC4 extends UserDefinedAggregateFunction{
    //定义函数的入参的个数的数据结构
    override def inputSchema: StructType = new StructType()
      .add(new StructField("c4",DoubleType,true))
      .add(new StructField("c3",DoubleType,true))

    //多行数据依次做聚合时，每次的中间结果保存覆盖更新到哪个缓存变量中。
    override def bufferSchema: StructType = new StructType()
      .add(new StructField("buffer_c4",DoubleType,true))
    //最终的字段的数据返回类型
    override def dataType: DataType = DoubleType

    //保证每一次的计算是否一样
    override def deterministic: Boolean = true

    //对中间缓存变量做一些初始化的操作
    override def initialize(buffer: MutableAggregationBuffer): Unit = null

    //每一次都有一行数据进来，应该怎么对buffer进行更新。
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      if(null != input.get(0)){
        //只有c2=1时，才会进来。
        buffer(0)=input.getDouble(0)
      }else{
        val last_c4= buffer.getDouble(0)
        val this_c3=input.getDouble(1)
        val this_temp_c4 =(last_c4+this_c3)/2
        buffer(0)=this_temp_c4
      }
    }

    //RDD的分区与分区之间的数据如何聚合，如果窗口函数有order by排序，就不涉及多个分区了。
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = null

    //最终的结果值是什么
    override def evaluate(buffer: Row): Any = buffer.getDouble(0)
  }



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
    //使用UDAF来计算后续的所有字段值
    spark.udf.register("UDAFC4",new UDAFC4())
    spark.sql(
      """
        |select c1,c2,c3,
        |       UDAFC4(c4,c3) over(partition by c1 order by c2) as c4
        | from mytab_temp
        |""".stripMargin).show()
    spark.close()

  }





}
