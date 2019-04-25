package com.retaildb.usecase

import org.apache.spark.sql.SparkSession
import  org.apache.spark.sql.functions._

object RetailDb extends App{

  val spark = SparkSession.builder().appName("retail db use case").enableHiveSupport().getOrCreate()

  val categoriesDf = readHive(spark, "categories")
  val customersDf = readHive(spark,"customers")
  val departmentsDf = readHive(spark, "departments")
  val orderItemsDf = readHive(spark, "orderItems")
  val ordersDf = readHive(spark, "orders")
  val productsDf = readHive(spark, "products")

  def readHive(session: SparkSession, tableName: String) = spark.read.table(tableName)

}
