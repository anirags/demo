import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object ParquetFileToCsv {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("Parquet")
      .config("spark.hadoop.validateOutputSpecs", "false")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read.format("csv")
      .option("delimiter", ",")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("mode", "DROPMALFORMED")
      .option("path", "D:\\Spark Project\\demo\\data\\MOCK_DATADemo.csv")
      .load()



    df.write.mode("Overwrite").csv("D:\\Spark Project\\demo\\src\\resources\\csv")
//   df.write.option("compression", "none")
//     .format("parquet")
//     .mode(SaveMode.Overwrite)
//     .save("D:\\Spark Project\\demo\\src\\resources\\parquet_data")

    spark.close()
  }
}
