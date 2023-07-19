package com.sundogsoftware.spark
import org.apache.spark.sql.SparkSession
object SecondFile {




    def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder().appName("SecondFile").getOrCreate()

      // Call the main function of FirstFile to get the DataFrame
      val eDF = LoadData.main(Array.empty[String])


      // Now you can use the DataFrame in your code


      // Stop the SparkSession when done
      spark.stop()

  }


}
