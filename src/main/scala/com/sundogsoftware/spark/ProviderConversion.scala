package com.sundogsoftware.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.DriverManager

object ProviderConversion {

  def parseLine(line:String):(String, String, String) =
  {
      val col = line.split("~")
      val col1 = col(0)
      val col2 = col(1)
      val col3 = col(2)
    (col1,col2, col3)
  }

  def main(args:Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

//    val sparkSession = SparkSession
//      .builder().appName("ProviderConversion")
//      .master("local")
//      .getOrCreate()

    val conf = new SparkConf().setAppName("Read DAT File").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd = sc.textFile("data/WHA_PROVIDER_20230515182549.dat")

    //remove headers
    val eRdd1 = rdd.mapPartitionsWithIndex {
      (idx, iter) => if (idx == 0) iter.drop(1) else iter
    }
    val rdd1 = rdd.map(parseLine)

    val rdd2 = rdd1.distinct()
    val rdd3 = rdd2.collect().map{ case (entity,typ,fg) => (entity, if(typ == "ORGANIZATION" || typ == "FACILITY") "Business" else typ , fg)

    }

  //  eRdd1.foreach(println)
    //rdd3.collect().foreach(println)

    //Convert each of rdd3 to a tab separated string
//    val rows = rdd3.collect().map(row => row.productIterator.mkString(" "))
//    val rows1 = rows.collect()

//    rows.foreach(println)
    //MySQL connection properties
    val url = "jdbc:mysql://localhost:3306/demo"
    val user = "root"
    val pass = "root"

    //table name in demo database
    val tableName = "provider"
    val columnNames = Seq("entity_id", "entity_type", "org_name")
    //create connection and Insert data in MySQL table

    val connection = DriverManager.getConnection(url, user,pass)

    rdd3.foreach(row => {
      val statement = connection.prepareStatement(s"insert into $tableName(${columnNames.mkString(", ")}) values(?,?,?)")
      statement.setString(1,row._1)
      statement.setString(2,row._2)
      statement.setString(3,row._3)
      statement.executeUpdate()
      statement.close()
    })

    //connection close
    connection.close()

    //session stop
    sc.stop()

  }

}
