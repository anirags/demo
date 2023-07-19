package com.sundogsoftware.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object YamahaPowerBikes  {

  def main (args:Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

//    val sparkSession = SparkSession.builder()
//      .master("local")
//      .appName("YamahaPowerBikes")
//      .getOrCreate()

//    val sourceDf = sparkSession.read.option("header", true).option("inferSchema", true).csv("data/Used_Bikes.csv")
//
//    sourceDf.show()

    val sc = new SparkContext("local[*]", "YamahaPowerBikes")

    val rdd1  = sc.textFile("data/Used_Bikes.csv")

    val rdd2 = rdd1.map(line => line.split(",").map(col => col.trim))

    val rdd3 = rdd2.filter(line => line(4).equalsIgnoreCase("First Owner")
      && line(6).toDouble > 150 && line(7).equalsIgnoreCase("Yamaha"))


    rdd3.foreach(arr => println(arr.mkString(", ")))

    println(rdd3.count())
  }
}
