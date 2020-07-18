package com.etl.assignment.report

import scala.collection.immutable.TreeMap

class FinalReporter extends Reporter [List[(String,(Double,Double))]]{

  override def print(records: List[(String,(Double,Double))]): Unit = {

    val countryWiseProdDetailData = records.groupBy(kv => kv._1)
      .map(kv => {
        val key = kv._1
        val barleyValue = kv._2(0)._2
        val beefValue = kv._2(1)._2
        val cottonValue = kv._2(2)._2
        val sb = new StringBuilder()
          .append(barleyValue._1).append("|").append(barleyValue._2)
          .append("|").append(beefValue._1).append("|").append(beefValue._2)
          .append("|").append(cottonValue._1).append("|").append(cottonValue._2)

        (key, sb.toString())
      })

    val reportMap = new TreeMap[String,String]() ++ countryWiseProdDetailData

    println("year|world_barley_harvest|usa_barley_contribution%|world_beef_slaughter|usa_beef_contribution%|world_cotton_harvest|usa_cotton_contribution%")
    reportMap.foreach(kv => {
      println(kv._1+"|"+kv._2)
    })
  }
}
