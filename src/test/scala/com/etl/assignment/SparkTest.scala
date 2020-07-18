
package com.etl.assignment

import com.etl.assignment.config.JobContext
import com.etl.assignment.model.{Barley, Beef, Cotton}
import com.etl.assignment.processor.SparkProcessor
import com.etl.assignment.reader.SparkCsvReader
import com.etl.assignment.report.SparkFinalReport
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}

class SparkTest extends FunSuite with BeforeAndAfter{

  var spark: SparkSession = _
  var jobContext: JobContext = _

  before{
    jobContext = new JobContext()
    spark = jobContext.spark
  }

  after{
    spark.close()
  }

  test("Spark-reader-test") {

    val reader = new SparkCsvReader( new JobContext(),"src/main/resources")
    reader.read().show()
  }

  test("processor-test" ){

    val path = "src/main/resources"

    val reader = new SparkCsvReader(new JobContext, path).read()

    val processor = new SparkProcessor().process(reader)

    processor.show()
  }

  test("spark-writer-test") {

    val path = "src/main/resources"

    val reader = new SparkCsvReader(new JobContext, path).read()

    val processor = new SparkProcessor().process(reader)

    val writer = new SparkFinalReport()
    writer.print(processor)
  }


}


