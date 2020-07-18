package com.etl.assignment.reader

object ReaderFactory {

  def getReader(readType: String): Object = {

    readType match {
      case "" => new Object
    }

  }

}
