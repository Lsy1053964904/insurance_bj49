package cn.itcast.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, StructField, StructType}

/**
 * @program: insurance_jb49
 * @description
 * @author: chenjia868
 * @create: 2021-05-08 15:07
 * */
class UDAFDxdDxciLxd extends UserDefinedAggregateFunction{
  //定义函数的入参的个数的数据结构
  override def inputSchema: StructType = new StructType()
    .add(new StructField("lx_d",DoubleType,true))
    .add(new StructField("qx_d",DoubleType,true))
    .add(new StructField("qx_ci",DoubleType,true))


  //多行数据依次做聚合时，每次的中间结果保存覆盖更新到哪个缓存变量中。
  //buffer其实就是一个小容器，容器里有几个变量，都可以随时被更新
  override def bufferSchema: StructType = new StructType()
    .add(new StructField("buffer_lx_d",DoubleType,true))
    .add(new StructField("buffer_qx_d",DoubleType,true))
    .add(new StructField("buffer_qx_ci",DoubleType,true))
  //最终的字段的数据返回类型
  override def dataType: DataType = new StructType()
    .add(new StructField("lx_d",DoubleType,true))
    .add(new StructField("dx_d",DoubleType,true))
    .add(new StructField("dx_ci",DoubleType,true))

  //保证每一次的计算是否一样,幂等性
  override def deterministic: Boolean = true

  //对中间缓存变量做一些初始化的操作
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)=null
    buffer(1)=null
    buffer(2)=null
  }

  //每一次都有一行数据进来，应该怎么对buffer进行更新。
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val this_lx_d_field: Any = input.get(0)
    //只有第一行（保单年度=1），才会进来
    if(this_lx_d_field !=null){
      val this_lx_d: Double = input.getDouble(0)
      val this_qx_d: Double = input.getDouble(1)
      val this_qx_ci: Double = input.getDouble(2)

      buffer(0)=this_lx_d
      buffer(1)=this_qx_d
      buffer(2)=this_qx_ci
    }else{
      //计算当前轮次的lx_d
      val last_lx_d = buffer.getDouble(0)
      val last_dx_d = buffer.getDouble(1)
      val last_dx_ci = buffer.getDouble(2)
      val this_lx_d =  last_lx_d-last_dx_d-last_dx_ci

      val this_qx_d = input.getDouble(1)
      val this_dx_d = this_lx_d*this_qx_d

      val this_qx_ci = input.getDouble(2)
      val this_dx_ci = this_lx_d*this_qx_ci

      buffer(0)=this_lx_d
      buffer(1)=this_dx_d
      buffer(2)=this_dx_ci
    }

  }

  //RDD的分区与分区之间的数据如何聚合，如果窗口函数有order by排序，就不涉及多个分区了。
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = null

  //最终的结果值是什么
  override def evaluate(buffer: Row): Any = buffer
}
