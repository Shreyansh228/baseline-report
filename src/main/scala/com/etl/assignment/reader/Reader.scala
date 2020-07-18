package com.etl.assignment.reader

trait Reader [T] extends Serializable {

  def read(): T
}
