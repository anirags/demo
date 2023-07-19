package ecom

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import java.io.PrintStream

object ECOMAnalysis {

  def main(args:Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local", "ECOMAnalysis")

    val spark = SparkSession.builder().getOrCreate()

    val rdd = sc.textFile("data/ecom_data.csv")


    //remove headers from rdd
    val rdd1 = rdd.mapPartitionsWithIndex{
      (idx, itr) => if(idx == 0) itr.drop(1) else itr
    }

    val ePairRdd = rdd1.map{ l =>
//      val str = l.split(",")
//      val (oid,cid,qt,cp,sp) = (str(0), str(1), str(2).toInt, str(3).toDouble, str(4).toDouble)
//      val (ts,rating,pCat,pid,sp_type) = (str(5), str(6).toInt, str(7), str(8), str(9))
//      val (oStat,pWt,pHt,pLen,pWidth) = (str(10),str(11).toInt,str(12).toInt,str(13).toInt,str(14).toInt)
//      val (cCity,cState,sid,sCity,sState) = (str(15), str(16), str(17), str(18), str(19))
//      val (sInstal) = (str(20).toInt)
      val str = l.split(",")
      val (oid, cid, qt, cp, sp) = (str(0), str(1), str(2).toInt, str(3).toDouble, str(4).toDouble)
      val (ts, rating, pCat, pid, sp_type) = (str(5), str(6).toInt, str(7), str(8), str(9))
      val (oStat, pWt, pHt, pLen, pWidth) = (str(10), str(11).toInt, str(12).toInt, str(13).toInt, str(14).toInt)
      val (cCity, cState, sid, sCity, sState, sInstal) = (str(15), str(16), str(17), str(18), str(19), str(20))
//      val (sInstal) = (str(20))

      (oid,cid,qt,cp,sp,
      ts,rating,pCat,pid,sp_type,
      oStat,pWt,pHt,pLen,pWidth,
      cCity,cState,sid,sCity,sState,
      sInstal)
    }
    // Convert your RDD to an RDD of Rows
//    val ePairRdd = rdd1.map { l =>
//      val str = l.split(",")
//      val oid = str(0)
//      val cid = str(1)
//      val qt = str(2).toInt
//      val cp = str(3).toDouble
//      val sp = str(4).toDouble
//      val ts = str(5)
//      val rating = str(6).toInt
//      val pCat = str(7)
//      val pid = str(8)
//      val sp_type = str(9)
//      val oStat = str(10)
//      val pWt = str(11).toInt
//      val pHt = str(12).toInt
//      val pLen = str(13).toInt
//      val pWidth = str(14).toInt
//      val cCity = str(15)
//      val cState = str(16)
//      val sid = str(17)
//      val sCity = str(18)
//      val sState = str(19)
//      val sInstal = str(20).toInt
//
//      (oid, cid, qt, cp, sp, ts, rating, pCat, pid, sp_type, oStat, pWt, pHt, pLen, pWidth, cCity, cState, sid, sCity, sState, sInstal)
//    }
    val df = spark.createDataFrame(ePairRdd)

    val renamedDF = df.withColumnRenamed("_1","oid")
      .withColumnRenamed("_2", "cid")
      .withColumnRenamed("_3","qt")
      .withColumnRenamed("_4", "cp")
      .withColumnRenamed("_5", "sp")
      .withColumnRenamed("_6", "ts")
      .withColumnRenamed("_7", "rating")
      .withColumnRenamed("_8","pCat")
      .withColumnRenamed("_9", "pid")
      .withColumnRenamed("_10", "sp_type")
      .withColumnRenamed("_11", "oState")
      .withColumnRenamed("_12", "pWt")
      .withColumnRenamed("_13", "pHt")
      .withColumnRenamed("_14","pLen")
      .withColumnRenamed("_15", "pWidth")
      .withColumnRenamed("_16", "cCity")
      .withColumnRenamed("_17","cState")
      .withColumnRenamed("_18", "sid")
      .withColumnRenamed("_19", "sCity")
      .withColumnRenamed("_20", "sState")
      .withColumnRenamed("_21", "sInstal")

    val ePairRdd2 = ePairRdd.map { case (oid, cid, qt, cp, sp, ts, rating, pCat, pId, sp_type,
    oStat, pWt, pLen, pHt, pWidth, cCity,
    cState, sId, sCity, sState, sInstal) => (cid, qt, sp)
    }

    val ePairRdd3 = ePairRdd2.map{ case(cid, qt,sp) =>
      val amount = qt*sp
      val amount_roundOff = (math rint amount*100)/100
      (cid, amount_roundOff)
    }.reduceByKey(_+_).sortBy(_._2, false)

    val all_customer = ePairRdd3.map{ case(cid , amount) =>
      if(amount >= 50000) {("VIP Customers", List((cid, amount)))}
      else if((amount >= 20000)&&(amount<50000)) {("Silver Customers", List((cid, amount)))}
      else if((amount >= 5000)&&(amount<20000)) {("Bronze Customers", List((cid,amount)))}
      else {("Lowtime Customers", List((cid, amount)))}
    }.reduceByKey(_++_)

//    all_customer.foreach(println)

    val all_customer_count = all_customer.map{ case(customer_type,list) => (customer_type,list.length)}.sortBy(_._2)

    val file = new PrintStream("src/resources/customer_type.csv")
    all_customer_count.collect.foreach{case (customer_type, count) => file.println(s"$customer_type,$count")}
    file.close()

    //monthly trend analysis

    val ePairRdd5 = ePairRdd.map {
      case (oid, cid, qt, cp, sp, ts, rating, pCat, pId, sp_type, oStat, pWt, pLen, pHt, pWidth, cCity, cState, sId, sCity, sState, sInstal) =>
        val ds_ts = ts.split(" ")
        val month = ds_ts(0).split("-")
        (pId,qt,month(1).toInt)
    }.sortBy(_._3)

    ePairRdd5.foreach(println)

  }

}

