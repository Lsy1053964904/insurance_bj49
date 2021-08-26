package cn.itcast.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, StructField, StructType}

/**
 * @program: insurance_jb49
 * @description
 * @author: chenjia868
 * @create: 2021-05-07 19:16
 * */
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
