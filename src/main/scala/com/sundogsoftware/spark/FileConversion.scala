package com.sundogsoftware.spark
import com.sundogsoftware.spark.Converter.writeDataFrameToCSV
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.jdk.CollectionConverters.seqAsJavaListConverter


object FileConversion {



  def main(args:Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkSession = SparkSession.builder()
      .appName("FileConversion")
      .master("local[*]")
      .getOrCreate()



    val df = sparkSession.read.option("multiline", true)
      .json("data/MOCK.json")

    val outputPath = "src/resources/deptData.csv"

//   val ndf = df.select("id", "first_name", "email", "dept")
    df.createOrReplaceTempView("deptdata")
    val transformedData = sparkSession.sql("select concat(first_name, ' ' ,last_name) as FullName, " +
      "substring(gender,1,1) as Gender,email, salary as salary, salary/12 as monthly_salary  from deptdata WHERE LENGTH(salary) > 6 AND LENGTH(salary) < 9 ")


   // transformedData.show()


    // Step 4: Extract the rows as List[String]
    val rows: List[String] = transformedData.collect().map(_.toSeq.mkString(",")).toList

    // Step 5: Convert List[String] to java.util.List[String]
    val javaList: java.util.List[String] = rows.asJava

    writeDataFrameToCSV(javaList,outputPath)

//    javaList.forEach(println)



//    df
//      .coalesce(1) // Optional: Coalesce to a single partition to have a single CSV file
//      .write
//      .format("csv")
//      .option("header", "true") // Include a header row in the CSV file
//      .save(outputPath)
    //ndf.printSchema()



    sparkSession.close()
  }

}
