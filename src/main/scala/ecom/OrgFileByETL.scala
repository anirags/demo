package ecom

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import java.util.Properties

object OrgFileByETL {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .appName("Dem").master("local[*]").getOrCreate()

    val path = "D:\\Spark Project\\demo\\src\\resources\\org"

    val keywords = List("business", "client","provider" )

    val connection = new Properties()
    connection.setProperty("user", "root")
    connection.setProperty("password", "root")
    connection.setProperty("driver", "com.mysql.cj.jdbc.Driver")

    val dbUrl = "jdbc:mysql://localhost:3306/prac"

    val files = new java.io.File(path).listFiles
    val csvFiles = files.filter(file => keywords.exists(keyword => file.getName.toLowerCase.contains(keyword)))

    csvFiles.foreach{ file =>
      val prefix = keywords.find(keyword => file.getName.toLowerCase.contains(keyword)).getOrElse("Unknown")

      val df = spark.read.option("header", true).csv(file.getAbsolutePath)

      df.write.mode("append").jdbc(dbUrl,prefix,connection)
    }

spark.stop()

  }

}
