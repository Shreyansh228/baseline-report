package com.etl.assignment.processor

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class SparkProcessor extends Processor[DataFrame, DataFrame] {

  override def process(data: DataFrame): DataFrame = {

    data.select(
      col("year"),
      col("world_barley_harvest"),
      (col("usa_barley_harvest") / col("world_barley_harvest")).as("usa_barley_contribution_per"),
      col("world_beef_slaughter"),
      (col("usa_beef_slaughter") / col("world_beef_slaughter")).as("usa_beef_contribution_per"),
      col("world_cotton_harvest"),
      (col("usa_cotton_harvest") / col("world_cotton_harvest")).as("usa_usa_contribution_per")
    )

  }
}
