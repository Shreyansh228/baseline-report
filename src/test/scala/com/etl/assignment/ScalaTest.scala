package com.etl.assignment

import java.io.File

import com.etl.assignment.constants.Constants.{BARLEY, BEEF, COTTON, USA}
import com.etl.assignment.model.{Barley, Beef, Cotton, Product}
import com.etl.assignment.processor.CountryWiseProductProcessor
import com.etl.assignment.reader.CsvReader
import com.etl.assignment.report.ConsoleReporter
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.collection._
import scala.io.Source

class ScalaTest extends FunSuite with BeforeAndAfter {

  test("csvReader-test") {

    val inputReadData = new CsvReader("/").read()
    inputReadData.foreach(country => {
      println(country._1,country._2)
    })


  }

  test("processing-test") {
    val inputReadData = new CsvReader("/").read()

    val usaBarleyHarvestData = new CountryWiseProductProcessor(USA, BARLEY)
      .process(inputReadData)
      .mapValues(_._2)

    usaBarleyHarvestData.foreach(println)
  }

  test("reporting-test") {

    val inputReadData = new CsvReader("/").read()

    val usaBarleyHarvestData = new CountryWiseProductProcessor(USA, BARLEY)
      .process(inputReadData)
      .mapValues(_._2)

    val report = new ConsoleReporter()

    report.print(usaBarleyHarvestData)
  }

}
