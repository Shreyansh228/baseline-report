package com.etl.assignment.processor

trait Processor[I,O] extends Serializable {

  def process(data: I): O
}
