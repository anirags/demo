package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object JsonToMySql {

 // case class Person(id: Int, first_name: String, last_name: String, email: String, gender: String, salary: Int, dept: String)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkSession = SparkSession
      .builder()
      .appName("LoadToMySQL")
      .master("local[*]")
      .getOrCreate()


//    val customerOrdersSchema = new StructType()
//      .add("id", IntegerType, nullable = true)
//      .add("first_name","String", nullable = true)
//      .add("last_name", "String", nullable = true)
//      .add("email", "String", nullable = true)
//      .add("gender", "String", nullable = true)
//      .add("salary", LongType, nullable = true)
//      .add("dept", "String", nullable = true)

    val df = sparkSession
      .read
      .option("multiline", true)
      //.schema(customerOrdersSchema)
      .json("data/MOCK.json")

    //df.show(numRows = 2000)

//    val result = df.groupBy("dept")
//      .agg(sum("salary").alias("Total_salary_by_dept"))
//
//    val finalResult = result.withColumn("Total_salary_by_dept", col("Total_salary_by_dept").cast(IntegerType))

    val result = df.groupBy("dept").agg(sum("salary").alias("salary"))


    val finalresult = result.withColumn("salary", col("salary").cast(IntegerType))

    //connection to demo database
    val jdbcHostname = "localhost" // Replace with your MySQL server hostname
    val jdbcPort = 3306 // Replace with your MySQL server port
    val jdbcDatabase = "demo" // Replace with your MySQL database name
    val jdbcUrl = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}"
    val jdbcUsername = "root" // Replace with your MySQL username
    val jdbcPassword = "root" // Replace with your MySQL password

    val tableName = "deptSalData" // Replace with your MySQL table name

    //writing data of result to deptSalData table
    finalresult.write
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("dbtable", tableName)
      .option("user", jdbcUsername)
      .option("password", jdbcPassword)
      .mode("append") // Change to "overwrite" if you want to replace the data in the table
      .save()


  }

}
