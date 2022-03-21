package com.tunan.tag.hbase

import com.tunan.tag.utils.HBaseTools
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object HBaseSQLTestForShortName {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[4]")
//      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    // 读取数据
    val usersDF: DataFrame = spark.read
      .format("hbase")
      .option("zkHosts", "aliyun")
      .option("zkPort", "2181")
      .option("hbaseTable", "user")
      .option("family", "info")
      .option("selectFields", "name,age,height,weight,phone")
      .load()
    usersDF.printSchema()
//    usersDF.cache()
//    usersDF.show(10, truncate = false)
//
//    HBaseTools.write(usersDF,"aliyun","2181","user","info","phone")

    usersDF.createOrReplaceTempView("t")

    val df = spark.sql("select name,'001' AS age,height,weight,phone from t")

    // 保存数据
    df.write
      .mode(SaveMode.Overwrite)
      .format("hbase")
      .option("zkHosts", "aliyun")
      .option("zkPort", "2181")
      .option("hbaseTable", "user")
      .option("family", "info")
      .option("rowKeyColumn", "phone")
      .save()
    spark.stop()
  }
}