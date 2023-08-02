package ecom

import org.apache.spark.sql.SparkSession

object Dem {

  def main(args:Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Dem").master("local[*]").getOrCreate()

    println("xys")

  }

}
