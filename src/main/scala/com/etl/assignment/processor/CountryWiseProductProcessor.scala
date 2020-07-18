package com.etl.assignment.processor

import com.etl.assignment.model.Product

import scala.collection.mutable

class CountryWiseProductProcessor(var country: String, product: String) extends Processor[mutable.HashMap[String, mutable.HashMap[String, List[Product]]], Map[String, (Double, Double)]] {

  override def process(data: mutable.HashMap[String, mutable.HashMap[String, List[Product]]]): Map[String, (Double, Double)] = {

    val usaBarleyData = ProcessorUtil.harvestProcessor(country, product, data)
    val worldBarleyData = ProcessorUtil.harvestProcessor("world", product, data)

    val unionHarvest = (usaBarleyData ++ worldBarleyData)

    val perHarvest = unionHarvest.groupBy(kv => kv._1)
      .map(kv => {
        val harvest = kv._2.map(fig => fig._2)
        val usa: Double = harvest(0)
        val world: Double = harvest(1)
        val value: Double = usa / world
        val finalValue: Double = value * 100
        (kv._1, (world, finalValue))
      })
    perHarvest
  }

}
