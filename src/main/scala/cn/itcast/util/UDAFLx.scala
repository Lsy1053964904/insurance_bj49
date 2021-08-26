package cn.itcast.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, StructField, StructType}

/**
 * @program: insurance_jb49
 * @description
 * @author: chenjia868
 * @create: 2021-05-08 10:31
 * */
class UDAFLx extends UserDefinedAggregateFunction{
  //定义函数的入参的个数的数据结构
  override def inputSchema: StructType = new StructType()
    .add(new StructField("lx",DoubleType,true))
    .add(new StructField("qx",DoubleType,true))

  //多行数据依次做聚合时，每次的中间结果保存覆盖更新到哪个缓存变量中。
  //buffer其实就是一个小容器，容器里有几个变量，都可以随时被更新
  override def bufferSchema: StructType = new StructType()
    .add(new StructField("buffer_lx",DoubleType,true))
    .add(new StructField("buffer_qx",DoubleType,true))
  //最终的字段的数据返回类型
  override def dataType: DataType = DoubleType

  //保证每一次的计算是否一样,幂等性
  override def deterministic: Boolean = true

  //对中间缓存变量做一些初始化的操作
  override def initialize(buffer: MutableAggregationBuffer): Unit = null

  //每一次都有一行数据进来，应该怎么对buffer进行更新。
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //是第一行
    //获取input的第一个入参lx
    val lx_obj = input.get(0)
    if(null != lx_obj){
      //把第一行的lx给buffer的buffer_lx变量
      buffer(0) = input.getDouble(0)
      //把第一行的qx给buffer的buffer_qx变量
      buffer(1) = input.getDouble(1)
      //以便给下一次使用。
    }else{
      //计算当前应由的lx是什么
      val last_lx=buffer.getDouble(0)
      val last_qx=buffer.getDouble(1)
      val this_lx=last_lx*(1-last_qx)

      val this_qx = input.getDouble(1)
      //以便给下一次使用。
      buffer(0) = this_lx
      buffer(1) = this_qx
    }
  }

  //RDD的分区与分区之间的数据如何聚合，如果窗口函数有order by排序，就不涉及多个分区了。
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = null

  //最终的结果值是什么
  override def evaluate(buffer: Row): Any = buffer.getDouble(0)
}
