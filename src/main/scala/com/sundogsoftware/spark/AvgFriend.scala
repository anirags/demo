package com.sundogsoftware.spark


import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AvgFriend {

  case class Person(id:Int, name:String, age:Int, friends:Int)

  def main(args:Array[String]){

    Logger.getLogger("org").setLevel(Level.ERROR)

    //spark session
    val spark = SparkSession.builder.appName("SparkSQL").master("local[*]").getOrCreate()

    // data set
    import spark.implicits._
    val data = spark.read.option("header", "true").option("inferSchema", "true").csv("data/fakefriends.csv")//.as[Person]

    data.groupBy("age").agg(round(avg("friends"),2).alias("avg_friends")).sort("age")
      .show()
  }

}
