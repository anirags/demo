package com.sundogsoftware.spark

import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object dataFrameCreateExmple {

  def main(args:Array[String]){

    val spark = SparkSession.builder.appName("DataFrame").master("local[*]").getOrCreate()
    val data = Seq((1,"anil"),(2,"ani"))

    val col = Seq("Name", "id")
    import spark.implicits._
    val df = spark.createDataFrame(data).toDF(col: _*)

    df.show()
  }

}
