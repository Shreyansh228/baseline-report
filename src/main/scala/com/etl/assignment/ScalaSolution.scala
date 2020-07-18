package com.etl.assignment

import com.etl.assignment.processor.CountryWiseProductProcessor
import com.etl.assignment.reader.CsvReader
import com.etl.assignment.report.{ConsoleReporter, FinalReporter}
import com.etl.assignment.constants.Constants._

object ScalaSolution {

  def main(args: Array[String]): Unit = {

    val inputReadData = new CsvReader("/").read()

    val listOfProducts = List(BARLEY, BEEF, COTTON)

    val result = listOfProducts.flatMap(product => {

      val usaBarleyHarvestData = new CountryWiseProductProcessor(USA, product)
        .process(inputReadData)

      usaBarleyHarvestData.map(kv => (kv._1.substring(0, 4), kv._2))
    })

    val report = new FinalReporter()
    report.print(result)

  }

}
/* ==> Sample output
year|world_barley_harvest|usa_barley_contribution%|world_beef_slaughter|usa_beef_contribution%|world_cotton_harvest|usa_cotton_contribution%
2016|49293.0|2.1057756679447386|236869.0|0.0|29708.0|12.952740002692877
2017|48122.0|1.6437388304725489|242741.0|0.0|33408.0|13.445881226053642
2018|48759.0|1.6407227383662504|246795.0|0.0|33200.0|12.831325301204819
2019|49890.0|1.7839246341952295|248547.0|0.0|34241.0|14.532285856137378
2020|50069.0|1.7775469851604786|262541.0|0.0|34222.0|13.128981357021798
2021|50171.0|1.855653664467521|265166.0|0.0|34915.0|12.87412286982672
2022|50238.0|1.8531788685855328|267679.0|0.0|36117.0|12.451200265802807
2023|50393.0|1.9268549203262357|270155.0|0.0|36521.0|12.431203964842146
2024|50477.0|1.9236483943182046|272532.0|0.0|36708.0|12.482292688242346
2025|50604.0|1.9188206465892026|274554.0|0.0|36972.0|12.509466623390676
2026|50772.0|1.9124714409517056|276638.0|0.0|37138.0|12.566643330281652
2027|50877.0|1.908524480610099|278708.0|0.0|37402.0|12.592909470081814
2028|50996.0|1.9040709075221585|280682.0|0.0|37696.0|12.600806451612904
 */