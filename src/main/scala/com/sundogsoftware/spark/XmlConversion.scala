package com.sundogsoftware.spark

import com.databricks.spark.xml._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
object XmlConversion {




  def main(args:Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val session = SparkSession
      .builder()
      .appName("XmlConversion")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .master("local[*]")
      .getOrCreate()

    val df = session.read
      .option("inferSchema", true)
      .option("header", true)
      .csv("data/data2.csv")


    df.write.format("xml").save("data/kjfj")









  }

}
