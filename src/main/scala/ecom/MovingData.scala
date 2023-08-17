package ecom

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

object MovingData {

  def main(args:Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "ECO")

    val spark = SparkSession.builder().getOrCreate()
    val file = "D:\\Spark Project\\demo\\src\\resources\\org\\WHA_PROVIDER_20230724215255.dat"

    val df = spark.read.option("delimiter", "~")
      .option("header", "true") // Set this to "true" if the first row contains column headers
      .option("inferSchema", "true") // Infers data types of columns
      .format("csv") // Use the CSV format
      .load(file)

//    df.createOrReplaceTempView("my_temp_view")
//    val query = "UPDATE my_temp_view SET LAST_NM = ORGANIZATION_NM, ORGANIZATION_NM = NULL WHERE ENTITY_TYPE = 'FACILITY';"
//
//    val df1 = spark.sql(query)

    val updatedDataFrame = df.withColumn("LAST_NM",
      when(col("ENTITY_TYPE") === "FACILITY", col("ORGANIZATION_NM")).otherwise(col("LAST_NM"))
    ).withColumn("ORGANIZATION_NM",
      when(col("ENTITY_TYPE") === "FACILITY", lit(null)).otherwise(col("ORGANIZATION_NM"))
    )

    val outputpath = "src/resources/who1.dat"
    writeDataFrameToDAT(updatedDataFrame,outputpath)
//    def writeDataFrameToDAT(dataFrame: DataFrame, filePath: String): Unit = {
//      val delimiter = "~"
//      val dataRows = dataFrame.rdd.map(row => row.mkString(delimiter))
//      dataRows.saveAsTextFile(filePath)

      def writeDataFrameToDAT(dataFrame: DataFrame, filePath: String, delimiter: String = "~"): Unit = {
        val header = dataFrame.columns.mkString(delimiter)
        val dataRows = dataFrame.rdd.map(row => row.mkString(delimiter))
//        val allRows = dataRows.map(row => s"$header$delimiter$row")
//        allRows.saveAsTextFile(filePath)

        val allData = Seq(header) ++ dataRows.collect()
        sc.parallelize(allData).repartition(1).saveAsTextFile(filePath)

      }

  }
}
