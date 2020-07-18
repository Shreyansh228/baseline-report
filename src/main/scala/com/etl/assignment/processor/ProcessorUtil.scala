package com.etl.assignment.processor

import com.etl.assignment.model.{Barley, Beef, Cotton, Product}

import scala.collection.mutable
import scala.util.Try
import com.etl.assignment.constants.Constants._

object ProcessorUtil  {

  def harvestProcessor(country: String, product: String, dataRecord : mutable.HashMap[String, mutable.HashMap[String, List[Product]]]) = {


    val usaItemData = dataRecord.get(country).get
      .get(product).get
      .map(b => {
        product match {
          case BARLEY => {
            val barley = b.asInstanceOf[Barley]
            (barley.year,Try{barley.harvest.toDouble}.toOption.getOrElse(0d))
          }
          case BEEF => {
            val beef = b.asInstanceOf[Beef]
            (beef.year,Try{beef.slaughter.toDouble}.toOption.getOrElse(0d))
          }
          case COTTON => {
            val cotton = b.asInstanceOf[Cotton]
            (cotton.year,Try{cotton.harvest.toDouble}.toOption.getOrElse(0d))
          }
        }

      })

    usaItemData


  }

}
