package com.etl.assignment

import com.etl.assignment.processor.{CountryWiseProductProcessor, CsvProcessor}
import com.etl.assignment.reader.{CsvReader, Reader}
import com.etl.assignment.report.ConsoleReporter
import com.etl.assignment.constants.Constants._


object ScalaSolutionBarley {

  def main(args: Array[String]): Unit = {

    val inputReadData = new CsvReader("/").read()

    val usaBarleyHarvestData = new CountryWiseProductProcessor(USA, BARLEY)
      .process(inputReadData)
      .mapValues(_._2)

    val report = new ConsoleReporter()

    report.print(usaBarleyHarvestData)

  }

}

/* ==> Sample output for usa/world barley harvest
(year,usa_barley_contribution_per)
(2016/17,2.1057756679447386)
(2017/18,1.6437388304725489)
(2018/19,1.6407227383662504)
(2019/20,1.7839246341952295)
(2020/21,1.7775469851604786)
(2021/22,1.855653664467521)
(2022/23,1.8531788685855328)
(2023/24,1.9268549203262357)
(2024/25,1.9236483943182046)
(2025/26,1.9188206465892026)
(2026/27,1.9124714409517056)
(2027/28,1.908524480610099)
(2028/29,1.9040709075221585)*/