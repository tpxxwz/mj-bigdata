package com.mj

import org.apache.spark.sql.SparkSession

import java.io.File

/**
 * @授课老师: 码界探索
 * @微信: 252810631
 * @版权所有: 请尊重劳动成果
 * IcebergWrite
 */
object IcebergWriteFrame {
  def main(args: Array[String]): Unit = {
    deleteDir(new File("D:/demo/spark/"))
    val spark: SparkSession = SparkSession.builder().master("local").appName("IcebergWriteFrame")
      .config("spark.sql.catalog.ice_hdfs", "org.apache.paimon.spark.SparkCatalog")
      .config("spark.sql.catalog.ice_hdfs.type", "hadoop")
      .config("spark.sql.catalog.ice_hdfs.warehouse", "file:///D:/demo/spark/")
      .getOrCreate()
    spark.sql(
      """
        |CREATE database IF NOT EXISTS ice_hdfs.mj
      """.stripMargin)
    //1.创建Iceberg表，并插入数据
    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS ice_hdfs.mj.t (id int, name string, age int)
      """.stripMargin)
    import spark.implicits._
    // 创建示例数据
    val data = Seq(
      (1L, "iceberg", 5),
      (2L, "flink",12)
    ).toDF("id", "name", "age")
    // 写入数据
    data.writeTo("ice_hdfs.mj.t").append()
  }
  def deleteDir(file: File): Unit = {
    if (file.isDirectory) {
      Option(file.listFiles).foreach(_.foreach(deleteDir))
    }
    file.delete()
  }
}