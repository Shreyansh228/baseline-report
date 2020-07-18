package com.etl.assignment.reader

import com.etl.assignment.config.JobContext
import com.etl.assignment.model.{Barley, Beef, Cotton}
import org.apache.spark.sql.DataFrame

import scala.collection.mutable
import com.etl.assignment.constants.Constants._


class SparkCsvReader(jobContext: JobContext, path: String) extends Reader[DataFrame] {

  val spark  = jobContext.spark

  override def read(): DataFrame = {

    import spark.implicits._
    import org.apache.spark.sql.functions._


    def nvludf(input: String): Double = {
      if (input == null || input == "--") {
        0d
      } else {
        input.toDouble
      }
    }

    val to_double = udf(nvludf _)

    spark.udf.register("to_double", to_double)

    val usaBarleyDF = spark.read.option("header", "true")
      .option("inferSchema", "false")
      .csv(s"${path}/usa/barley/*").as[Barley]
      .select(substring(col("year"), 0, 4).as("year"), to_double(col("harvest")).as("harvest"))
    //.withColumn("product",lit("barley"))

    val usaBeefDF = spark.read.option("header", "true")
      .option("inferSchema", "false")
      .csv(s"${path}/usa/beef/*").as[Beef]
      .select(substring(col("year"), 0, 4).as("year"), to_double(col("slaughter")).as("slaughter"))
    //.withColumn("product",lit("beef"))


    val usaCottonDF = spark.read.option("header", "true")
      .option("inferSchema", "false")
      .csv(s"${path}/usa/cotton/*").as[Cotton]
      .select(substring(col("year"), 0, 4).as("year"), to_double(col("harvest")).as("harvest"))
    //.withColumn("product",lit("cotton"))


    val worldBarleyDF = spark.read.option("header", "true")
      .option("inferSchema", "false")
      .csv(s"${path}/world/barley/*").as[Barley]
      .select(substring(col("year"), 0, 4).as("year"), to_double(col("harvest")).as("harvest"))
    //.withColumn("product",lit("barley"))


    val worldBeefDF = spark.read.option("header", "true")
      .option("inferSchema", "false")
      .csv(s"${path}/world/beef/*").as[Beef]
      .select(substring(col("year"), 0, 4).as("year"), to_double(col("slaughter")).as("slaughter"))
    //.withColumn("product",lit("beef"))

    val worldCottonDF = spark.read.option("header", "true")
      .option("inferSchema", "false")
      .csv(s"${path}/world/cotton/*").as[Cotton]
      .select(substring(col("year"), 0, 4).as("year"), to_double(col("harvest")).as("harvest"))
    //.withColumn("product",lit("cotton"))


    val usaToWorldBarleyDF = usaBarleyDF.join(worldBarleyDF, usaBarleyDF("year") === worldBarleyDF("year"))
      .select(usaBarleyDF("year"), usaBarleyDF("harvest").as("usa_barley_harvest"),
        worldBarleyDF("harvest").as("world_barley_harvest"))


    val usaToWorldBeefDF = usaBeefDF.join(worldBeefDF, usaBeefDF("year") === worldBeefDF("year"))
      .select(usaBeefDF("year"), usaBeefDF("slaughter").as("usa_beef_slaughter"),
        worldBeefDF("slaughter").as("world_beef_slaughter"))

    val usaToWorldCottonDF = usaCottonDF.join(worldCottonDF, usaCottonDF("year") === worldCottonDF("year"))
      .select(usaCottonDF("year"), usaCottonDF("harvest").as("usa_cotton_harvest"),
        worldCottonDF("harvest").as("world_cotton_harvest"))

    val consolidatedDF = usaToWorldBarleyDF
      .join(usaToWorldBeefDF, usaToWorldBarleyDF("year") === usaToWorldBeefDF("year"))
      .join(usaToWorldCottonDF, usaToWorldBarleyDF("year") === usaToWorldCottonDF("year"))
      .select(usaToWorldBarleyDF("year"), usaToWorldBarleyDF("world_barley_harvest"), usaToWorldBarleyDF("usa_barley_harvest"),
        usaToWorldBeefDF("world_beef_slaughter"), usaToWorldBeefDF("usa_beef_slaughter"),
        usaToWorldCottonDF("world_cotton_harvest"), usaToWorldCottonDF("usa_cotton_harvest")
      )

    consolidatedDF
  }
}
