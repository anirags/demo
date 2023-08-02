package com.sundogsoftware.spark
import com.databricks.spark.xml._
import org.apache.hadoop.fs._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StructType}

object CsvToCsv {


  def main(args:Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)



    val spark = SparkSession.builder()
      .appName("CSVTOCSV")
      .config("spark.sql.shuffle.partitions","200")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val mockSchema = new StructType()
      .add("id", IntegerType, nullable = true)
      .add("first_name", "String", nullable = true)
      .add("last_name", "String", nullable = true)
      .add("email", "String", nullable = true)
      .add("gender", "String", nullable = true)
      .add("salary", IntegerType, nullable = true)
      .add("dept", "String", nullable = true)

//   val df = spark.read.option("sep", ",").option("header", true).schema(mockSchema).csv("data/MOCK_DATADemo.csv")
     val df = spark.read.option("sep", ",").option("header", true).option("inferSchema",true).csv("data/CIGNA_DIRECTORY1.csv")

    val numOfRec = 10000
//    val df = spark.read.option("sep", ",").option("header", true).option("inferSchema",true).csv("data/CIGNA_DIRECTORY.csv")

    val dataToWrite = df.limit(numOfRec)


    dataToWrite.repartition(1).write.option("header",true).mode("Overwrite").csv("src/resources/avl1")
//
//   df.printSchema()
//
//    //df.write.mode(saveMode = "append").csv("D:\\Spark Project\\demo\\src\\resources\\dat")
//    df.write.mode("Overwrite").csv("C:\\Users\\Anil.Bhallavi\\Desktop\\temp\\csv")

//    val txt = spark.read.text("D:\\Spark Project\\demo\\data\\textFile.txt")
//
//    txt.write.mode("append").text("D:\\Spark Project\\demo\\data")

//    val file = new PrintWriter("src/resources/cf1.parquet")
//    df.collect.foreach(row => file.println(row))
//    file.close()
//    file.close()
//
//    df.repartition(1).write
//      .format("com.databricks.spark.xml")
//      .option("rootTag", "root")
//      .option("rowTag", "record")
//      .save("src/resources/output.xml")

//    val nestedDF = df.groupBy("dept")
//      .agg(collect_list(struct(df.columns.map(col): _*)).alias("records"))
//
//        nestedDF.repartition(1).write
//          .format("com.databricks.spark.xml")
//          .option("rootTag", "root")
//          .option("rowTag", "record")
//          .save("src/resources/output1.xml")


  }

  }

