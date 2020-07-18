package com.etl.assignment

import com.etl.assignment.model.{Barley, Beef, Cotton}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Dump {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache").setLevel(Level.WARN)
    val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
    import org.apache.spark.sql.functions._
    import spark.implicits._

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
      .csv("src/main/resources/usa/barley/*").as[Barley]
      .select(substring(col("year"), 0, 4).as("year"), to_double(col("harvest")).as("harvest"))
    //.withColumn("product",lit("barley"))


    val usaBeefDF = spark.read.option("header", "true")
      .option("inferSchema", "false")
      .csv("src/main/resources/usa/beef/*").as[Beef]
      .select(substring(col("year"), 0, 4).as("year"), to_double(col("slaughter")).as("slaughter"))
    //.withColumn("product",lit("beef"))


    val usaCottonDF = spark.read.option("header", "true")
      .option("inferSchema", "false")
      .csv("src/main/resources/usa/cotton/*").as[Cotton]
      .select(substring(col("year"), 0, 4).as("year"), to_double(col("harvest")).as("harvest"))
    //.withColumn("product",lit("cotton"))


    val worldBarleyDF = spark.read.option("header", "true")
      .option("inferSchema", "false")
      .csv("src/main/resources/world/barley/*").as[Barley]
      .select(substring(col("year"), 0, 4).as("year"), to_double(col("harvest")).as("harvest"))
    //.withColumn("product",lit("barley"))


    val worldBeefDF = spark.read.option("header", "true")
      .option("inferSchema", "false")
      .csv("src/main/resources/world/beef/*").as[Beef]
      .select(substring(col("year"), 0, 4).as("year"), to_double(col("slaughter")).as("slaughter"))
    //.withColumn("product",lit("beef"))

    val worldCottonDF = spark.read.option("header", "true")
      .option("inferSchema", "false")
      .csv("src/main/resources/world/cotton/*").as[Cotton]
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


   // usaToWorldBarleyDF.show()
    //usaToWorldBeefDF.show()
    //usaToWorldCottonDF.show()

    val consolidatedDF = usaToWorldBarleyDF
      .join(usaToWorldBeefDF, usaToWorldBarleyDF("year") === usaToWorldBeefDF("year"))
      .join(usaToWorldCottonDF, usaToWorldBarleyDF("year") === usaToWorldCottonDF("year"))
      .select(usaToWorldBarleyDF("year"), usaToWorldBarleyDF("world_barley_harvest"), usaToWorldBarleyDF("usa_barley_harvest"),
        usaToWorldBeefDF("world_beef_slaughter"), usaToWorldBeefDF("usa_beef_slaughter"),
        usaToWorldCottonDF("world_cotton_harvest"), usaToWorldCottonDF("usa_cotton_harvest")
      )

    consolidatedDF.show()


    val processordDF = consolidatedDF.select(
      col("year"),
      col("world_barley_harvest"),
      ((col("usa_barley_harvest") * 100)/col("world_barley_harvest")).as("usa_barley_contribution_per"),
      col("world_beef_slaughter"),
      ((col("usa_beef_slaughter")* 100)/col("world_beef_slaughter")).as("usa_beef_contribution_per"),
      col("world_cotton_harvest"),
      ((col("usa_cotton_harvest")* 100)/col("world_cotton_harvest")).as("usa_usa_contribution_per")
    )
    processordDF.show()

    println(processordDF.columns.mkString("|"))
    processordDF.collect().foreach(row => {
      println(row.mkString("|"))
    })



    spark.close()
  }
}
