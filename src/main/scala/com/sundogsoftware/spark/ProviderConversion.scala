package com.sundogsoftware.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
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

//  def parseColumn (line:String):(String,String,String,String,String,String,String,String,String,String,String,
//    String,String,String,String,String,String,String,String,String,String,String,String,String,
//    String,String,String,String,String,String,String) ={
//    val col = line.split("~")
//    (col(0),col(1),col(2),col(3),col(4),col(5),col(6),col(7),col(8),col(9),col(10),col(11),col(12),col(13),col(14),col(15),
//    col(16),col(17),col(18),col(19),col(20),col(21),col(22),col(23),col(24),col(25),col(26),col(27),col(28),col(29),col(30))
//  }
  Seq("ENTITY_ID", "ENTITY_TYPE", "ORGANIZATION_NM", "DBA_NM", "FIRST_NM", "LAST_NM", "" +
    "MIDDLE_NM", "PREFIX", "SUFFIX", "PREFERRED_NM", "CREDENTIALS", "GENDER_CD", "DOB", "ETHNICITY_CD", "IS_DECEASED",
    "NOTIFICATION_EMAIL", "ATYPICAL_IN", "EFFECTIVE_DT", "EXPIRED_DT", "NO_MEDICAL_DIRECTOR_REASON", "PROVIDER_OWNED_ORGANIZATION",
    "EIN", "NPI", "NPI_TYPE_CD", "SSN", "PROVIDES_TELEHEALTH_SVCS", "PROVIDES_INHOME_SUPPORT_SVCS", "MHP_AREA_OF_EXPERTISE", "MHP_PRACTICE_FOCUS",
    "ACADEMIC_DEGREE_CD", "URL"
  )

  def main(args:Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

//    val sparkSession = SparkSession
//      .builder().appName("ProviderConversion")
//      .master("local")
//      .getOrCreate()

    val conf = new SparkConf().setAppName("Read DAT File").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd = sc.textFile("data/WHA_PROVIDER_20230515182549.dat")

    val spark = SparkSession.builder().getOrCreate()


    //remove headers
    val eRdd1 = rdd.mapPartitionsWithIndex {
      (idx, iter) => if (idx == 0) iter.drop(1) else iter
    }
    val rdd1 = rdd.map(parseLine)

//    val rdd1 = eRdd1.collect().map(a => a.split("~"))
    val rdd2 = rdd1.collect.distinct
    val rdd3 = rdd2.map{ case (entity,typ,fg) => (entity, if(typ == "ORGANIZATION" || typ == "FACILITY") "Business" else typ , fg)

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
    val tableName = "temp_provider"
    val columnNames = Seq("entity_id", "entity_type", "org_name")
    //create connection and Insert data in MySQL table

    val connection = DriverManager.getConnection(url, user,pass)
//    val statement = connection.prepareStatement(s"update $tableName set entity_id = '471756208' where entity_id = '471756207'")
//    statement.executeUpdate()
//    statement.close()
//    rdd1.foreach(row => {
//      val statement = connection.prepareStatement(s"insert into $tableName(${columnNames.mkString(", ")}) values(?,?,?)")
//      statement.setString(1,row._1)
//      statement.setString(2,row._2)
//      statement.setString(3,row._3)
//          statement.executeUpdate()
//          statement.close()
//    })

    val col = Seq("ENTITY_ID","ENTITY_TYPE","ORGANIZATION_NM","DBA_NM","FIRST_NM","LAST_NM","" +
      "MIDDLE_NM","PREFIX","SUFFIX","PREFERRED_NM","CREDENTIALS","GENDER_CD","DOB","ETHNICITY_CD","IS_DECEASED",
      "NOTIFICATION_EMAIL","ATYPICAL_IN","EFFECTIVE_DT","EXPIRED_DT","NO_MEDICAL_DIRECTOR_REASON","PROVIDER_OWNED_ORGANIZATION",
      "EIN","NPI","NPI_TYPE_CD","SSN","PROVIDES_TELEHEALTH_SVCS","PROVIDES_INHOME_SUPPORT_SVCS","MHP_AREA_OF_EXPERTISE","MHP_PRACTICE_FOCUS",
      "ACADEMIC_DEGREE_CD","URL"
    )



    val col1 = col.map(a => a.toLowerCase)

//      rdd1.foreach(row => {
//        val statement = connection.prepareStatement(s"insert into $tableName(${col1.mkString(", ")})values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
//        statement.setString(1, row(0))
//        statement.setString(2, row(1))
//        statement.setString(3, row(2))
//        statement.setString(4, row(3))
//        statement.setString(5, row(4))
//        statement.setString(6, row(5))
//        statement.setString(7, row(6))
//        statement.setString(8, row(7))
//        statement.setString(9, row(8))
//        statement.setString(10, row(9))
//        statement.setString(11, row(10))
//        statement.setString(12, row(11))
//        statement.setString(13, row(12))
//        statement.setString(14, row(13))
//        statement.setString(15, row(14))
//        statement.setString(16, row(15))
//        statement.setString(17, row(16))
//        statement.setString(18, row(17))
//        statement.setString(19, row(18))
//        statement.setString(20, row(19))
//        statement.setString(21, row(20))
//        statement.setString(22, row(21))
//        statement.setString(23, row(22))
//        statement.setString(24, row(23))
//        statement.setString(25, row(24))
//        statement.setString(26, row(25))
//        statement.setString(27, row(26))
//        statement.setString(28, row(27))
//        statement.setString(29, row(28))
//        statement.setString(30, row(29))
//        statement.setString(31, row(30))
//        statement.executeUpdate()
//        statement.close()
//      })
//

    //connection close
    connection.close()

    
    //session stop
    sc.stop()

  }

}
