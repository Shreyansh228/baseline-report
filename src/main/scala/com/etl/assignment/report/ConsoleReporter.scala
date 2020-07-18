package com.etl.assignment.report

import scala.collection.immutable.TreeMap

class ConsoleReporter extends Reporter [Map[String,Double]]{

  override def print(records: Map[String,Double]): Unit = {

    println("year","usa_barley_contribution_per")
    val result = new TreeMap[String,Double]() ++ records
    result.foreach(println)

  }
}
