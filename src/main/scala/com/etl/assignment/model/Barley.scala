package com.etl.assignment.model

class Product
case class Barley (year: String,harvest: String,byield: String,production: String,imports: String,exports: String,consumption: String,industrial_use: String,use: String,stocks: String) extends Product
case class Beef(year: String,slaughter: String,byield: String,production: String,imports: String,exports: String,consumption: String,stocks: String) extends Product
case class Cotton(year: String,harvest: String,cyield: String,production: String,imports: String,	exports: String,consumption: String,loss: String,stocks: String) extends Product
