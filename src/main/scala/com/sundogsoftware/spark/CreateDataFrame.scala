package com.sundogsoftware.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.sql.Row.empty.schema
import org.apache.spark.sql.SparkSession

object CreateDataFrame {

  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("StructuredStreaming")
      .master("local[*]")
      .getOrCreate()

  val data = Seq(("James", "", "Smith", "1991-04-01", "M", 3000),
    ("Michael", "Rose", "", "2000-05-19", "M", 4000),
    ("Robert", "", "Williams", "1978-09-05", "M", 4000),
    ("Maria", "Anne", "Jones", "1967-12-01", "F", 4000),
    ("Jen", "Mary", "Brown", "1980-02-17", "F", -1)
  )

  val columns = Seq("firstname", "middlename", "lastname", "dob", "gender", "salary")
  val df = spark.createDataFrame(data)
    .toDF(columns: _*)

    df.show()
  }


}
