package util

import org.apache.spark.sql._
import org.apache.spark.sql.functions.udf


object transformationRules {


  def add(a:String,b:String):Int =
  {
      val add = a.toInt + b.toInt
    add.toInt
  }

  val addUDF = udf(add _)



}
