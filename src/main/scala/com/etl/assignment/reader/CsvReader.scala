package com.etl.assignment.reader

import java.io.File

import com.etl.assignment.model.{Barley, Beef, Cotton, Product}

import scala.collection.mutable
import scala.io.Source
import com.etl.assignment.constants.Constants._


class CsvReader(var path: String) extends Reader [mutable.HashMap[String, mutable.HashMap[String, List[Product]]]]{


  override def read(): mutable.HashMap[String, mutable.HashMap[String, List[Product]]] = {

    getCountryWiseData()

  }

  def getCountryWiseData() ={


    val countryItemMap = new mutable.HashMap[String, mutable.HashMap[String, List[Product]]]()

    val sourcePath = getClass.getResource("/")
    val fileFolder = new File(sourcePath.getPath)

    //source file
    if (fileFolder.exists && fileFolder.isDirectory) {

      val countriesFile = fileFolder.listFiles.toList.filterNot(f => f.getName == "com" || f.getName.toLowerCase() == "meta-inf")


      //country file
      countriesFile.foreach(countryFolder => {

        if (countryFolder.exists && countryFolder.isDirectory) {
          val itemFile = countryFolder.listFiles.toList

          val productListMap = new mutable.HashMap[String, List[Product]]()
          //item file
          itemFile.foreach(itemFolder => {

            if (itemFolder.exists && itemFolder.isDirectory) {
              val dataFile = itemFolder.listFiles.toList

              val itemMap = new mutable.HashMap[String, List[Product]]()

              //datafile
              dataFile.foreach(csvFolder => {
                if (csvFolder.exists && csvFolder.isDirectory) {

                } else {

                  val lines = Source.fromFile(csvFolder.getPath).getLines.toList.drop(1)
                  itemMap += (itemFolder.getName.toLowerCase -> getProductBean(lines,itemFolder.getName))//.foreach(println)
                }
              })

              productListMap ++= itemMap
            }

          })
          countryItemMap += (countryFolder.getName.toLowerCase -> productListMap)
        }

      })
    }
/*
    countryItemMap.foreach(country => {
        println(country._1,country._2)
      })
    println("\n")*/
    countryItemMap
  }


  def getProductBean(list: List[String], product: String): List[Product] = {

    product.toLowerCase match {
      case BARLEY => {
        list.map(fig => {
          val arrFig = fig.split(",")//.map(c => Try{c.toInt}.toOption.getOrElse(0))
          Barley(arrFig(0),arrFig(1),arrFig(2),arrFig(3),arrFig(4),arrFig(5),arrFig(6),arrFig(7),arrFig(8),arrFig(9))
        })
      }
      case BEEF => {
        list.map(fig => {
          val arrFig = fig.split(",")//.map(c => Try{c.toInt}.toOption.getOrElse(0))
          Beef(arrFig(0),arrFig(1),arrFig(2),arrFig(3),arrFig(4),arrFig(5),arrFig(6),arrFig(7))
        })
      }
      case COTTON => {
        list.map(fig => {
          val arrFig = fig.split(",")//.map(c => Try{c.toInt}.toOption.getOrElse(0))
          Cotton(arrFig(0),arrFig(1),arrFig(2),arrFig(3),arrFig(4),arrFig(5),arrFig(6),arrFig(7),arrFig(8))
        })
      }
      case COTTON => {
        list.map(fig => {
          val arrFig = fig.split(",")//.map(c => Try{c.toInt}.toOption.getOrElse(0))
          Cotton(arrFig(0),arrFig(1),arrFig(2),arrFig(3),arrFig(4),arrFig(5),arrFig(6),arrFig(7),arrFig(8))
        })
      }
      case _ => {
        List.empty[Product]
      }
    }

  }
}
