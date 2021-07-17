package com.quantexa.assignments.transactions

object OutputFormat {
  case class Question1Result(
                              transactionDay: Int,
                              transactionAmount: Double
                            )
  case class Question2Result(
                              accountId: String,
                              categoryAvgValueMap: Map[String,Double]
                            )
  case class Question3Result(
                              transactionDay:Int,
                              accountId:String,
                              max:Double,
                              avg:Double,
                              aaTotal:Double,
                              ccTotal:Double,
                              ffTotal:Double
                            )

}
