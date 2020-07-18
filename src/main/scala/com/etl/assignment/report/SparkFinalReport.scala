package com.etl.assignment.report

import org.apache.spark.sql.DataFrame

class SparkFinalReport extends Reporter [DataFrame]{
  override def print(records: DataFrame): Unit = {

    println(records.columns.mkString("|"))
    records.collect().foreach(row => {
      println(row.mkString("|"))
    })

  }
}
