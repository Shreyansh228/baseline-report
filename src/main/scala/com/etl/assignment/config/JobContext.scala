package com.etl.assignment.config

import org.apache.spark.sql.SparkSession

class JobContext {

  val spark = SparkSession.builder().master("local[*]").appName("assignment").getOrCreate()



}
