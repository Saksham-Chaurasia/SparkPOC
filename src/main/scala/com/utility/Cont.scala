package com.utility

import org.apache.spark.sql.functions.udf


object Cont {
  def mapsts(entity_type_code: Int): String = {
    entity_type_code match {
      case 1 => "Individual"
      case 2 => "Organization"
      case _ => ""
    }
  }
  val entity = udf[String, Int](mapsts)

}


