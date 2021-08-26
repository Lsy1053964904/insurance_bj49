package cn.itcast.util

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.InputStream
import scala.io.Source

/**
 * @program: insurance_jb49
 * @description
 * @author: chenjia868
 * @create: 2021-05-06 18:07
 * */
object SparkUtil {
  /**
   * 创建一个公共的方法执行一个SQL文本的内容。做到SQL代码文件与scala代码文件分离，可读性高。
   * 并且大多数情况下，还可以在SQL代码文件中单独execute调试，不用run main方法（慢）。
   * @param spark
   * @param fielname
   */
  def executeSQLFile(spark:SparkSession, fielname:String)={
    //定义读取SQL文本文件的输入流
    val in: InputStream = this.getClass.getClassLoader().getResourceAsStream(fielname);
    //获取输入流的文本内容
    val sqls: String = Source.fromInputStream(in).mkString
    //将文本内容按分号; 切割，切割后的每个部分就是一个单独的SQL语句
    sqls.split(";")
      //多虑掉空白行
      .filter(StringUtils.isNoneBlank(_))
      .foreach(sql => {
        val start = System.currentTimeMillis()
        //打印每个SQL语句
        println(sql + ";")
        val df: DataFrame = spark.sql(sql)
        // 如果碰到select，我们想查看结果，可以打印结果，方便调试。
        // 其他的非select语句比如set xxx，create xxx，cache table xxx则不用打印
        // 需要过滤掉注释内容，以免干扰。SQL的注释的特点是以--开头，单独一行。
        if (sql.split("\n").filter(!_.trim.startsWith("--")).mkString.trim.startsWith("select")) {
          df.show()
        }
        val end = System.currentTimeMillis()
        //顺便打印每个SQL语句的执行时间。
        println("耗时:"+(end-start)/1000+"秒")
      })
  }

}
