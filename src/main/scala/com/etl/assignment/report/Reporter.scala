package com.etl.assignment.report

trait Reporter[T] extends Serializable {

  def print(records: T)
}
