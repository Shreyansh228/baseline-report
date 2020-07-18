package com.etl.assignment.processor

import com.etl.assignment.model.Product

import scala.collection.mutable

class CsvProcessor(var country: String) extends Processor [mutable.HashMap[String, mutable.HashMap[String, List[Product]]],mutable.HashMap[String, mutable.HashMap[String, List[Product]]]]{

  def this() {
    this(null)
  }
  override def process(data: mutable.HashMap[String, mutable.HashMap[String, List[Product]]]): mutable.HashMap[String, mutable.HashMap[String, List[Product]]] = {

    data.filter(k => k._1 == country)

  }


}
