package com.sundogsoftware.spark

import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType}
import org.apache.log4j._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
object CustomerSpent {

  case class Customer(id : Int,item_id:Int, amount:Float)

  def main(args:Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder.appName("CustomerSpent").master("local[*]").getOrCreate()

    val customerSchema = new StructType().add("id", IntegerType,nullable = true)
      .add("item_id",IntegerType,nullable = true)
      .add("amount",FloatType, nullable = true)

    import spark.implicits._
    val data = spark.read.schema(customerSchema).csv("data/customer-orders.csv").as[Customer]

     val customerData = data.select("id","amount")

     val aggData = customerData.groupBy("id").agg(round(sum("amount"),2).alias("amount_spent"))

     val sortedData = aggData.sort("amount_spent")

    sortedData.show(sortedData.count.toInt)


  }

}
