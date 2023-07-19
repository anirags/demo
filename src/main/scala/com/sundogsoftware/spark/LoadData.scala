package com.sundogsoftware.spark


import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql._

object LoadData {

  case class EData(oid: String, cid: String, qt: Int, cp: Float, sp: Float, ts: String, rating: Int, pCat: String, pId: String,
                   sp_type: String, oStat: String, pWt: Int, pLen: Int, pHt: Int, pWidth: Int, cCity: String, cState: String,
                   sId: String, sCity: String, sState: String, sInstal: Int)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "LoadData")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val eRdd = sc.textFile("data/ecom_data.csv")

    val eRdd1 = eRdd.mapPartitionsWithIndex {
      (idx, iter) => if (idx == 0) iter.drop(1) else iter
    }

    val ePairRdd = eRdd1.map { l =>
      val str0 = l.split(",")
      val (oid, cid, qt, cp, sp) = (str0(0), str0(1), str0(2).toInt, str0(3).toFloat, str0(4).toFloat)
      val (ts, rating, pCat, pId, sp_type) = (str0(5), str0(6).toInt, str0(7), str0(8), str0(9))
      val (oStat, pWt, pLen, pHt, pWidth) = (str0(10), str0(11).toInt, str0(12).toInt, str0(13).toInt, str0(14).toInt)
      val (cCity, cState, sId, sCity, sState) = (str0(15), str0(16), str0(17), str0(18), str0(19))
      val (sInstal) = (str0(20).toInt)

      (oid, cid, qt, cp, sp, ts, rating, pCat, pId, sp_type, oStat, pWt, pLen, pHt, pWidth, cCity, cState,
        sId, sCity, sState, sInstal)
    }
 ePairRdd.toDF("oid", "cid", "qt", "cp", "sp", "ts", "rating", "pCat", "pId", "sp_type",
      "oStat", "pWt", "pLen", "pHt", "pWidth", "cCity",
      "cState", "sId", "sCity", "sState", "sInstal")


    val ePairRdd2 = ePairRdd.map { case (oid, cid, qt, cp, sp, ts, rating, pCat, pId, sp_type,
    oStat, pWt, pLen, pHt, pWidth, cCity,
    cState, sId, sCity, sState, sInstal) => (cid, qt, sp)
    }

    val  ePairRdd4 = ePairRdd2.map{ case (cid, qt, sp) =>
      val amount = qt*sp
      val amount_roundoff = (math rint amount*100) / 100
      (cid, amount_roundoff)
    }.reduceByKey(_+_).sortBy(_._2, false)

    ePairRdd4.toDF("Customer ID", "Bill Amount").show()

    




  }

}

