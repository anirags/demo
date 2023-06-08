package com.sundogsoftware.spark

import org.apache.spark.sql._

import org.apache.log4j._

import org.apache.spark.sql.types._
object JoinDataset {

  def extractIdName(line: String): ( Int,String) = {
    val fields = line.split("\\|")
    val id = fields(0).toInt
    val name = fields(1)
    (id,name)
  }

   case class Mov(userID :Int , movieID : Int, rating : Int, timestamp:Long)

  def main(args: Array[String]) {


    Logger.getLogger("org").setLevel(Level.ERROR)


    val spark = SparkSession.builder.appName("join").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    val rating = sc.textFile("data/ml-100k/u.item")

    val reqRating = rating.map(extractIdName)

//    reqRating.foreach(println)

    val schema = new StructType()
      .add("movieID", IntegerType, nullable = true)
      .add("movieName", StringType, nullable = true)

    import spark.implicits._
    val movierat = reqRating.toDF("movieID","movieName")


    val moviesSchema = new StructType()
      .add("userID", IntegerType, nullable = true)
      .add("movieID", IntegerType, nullable = true)
      .add("rating", IntegerType, nullable = true)
      .add("timestamp", LongType, nullable = true)

    val movieData = spark.read.option("sep", "\t").schema(moviesSchema).csv("data/ml-100k/u.data")

   // movieData.show()
   val combineData = movierat.join(movieData,Seq("movieID"), "inner")
   //combineData.show()

//    val groupedData = combineData.groupBy("movieID").count().select("movieName")
//
//    groupedData.show(false)

    val result = combineData.groupBy("movieID", "movieName")
      .count()
      .select("movieID", "count", "movieName").orderBy($"count".desc)

    result.show()




  }
}