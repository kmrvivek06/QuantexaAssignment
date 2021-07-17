/*
  Quantexa Copyright Statement
 */

package com.quantexa.assignments.transactions

import scala.io.Source
import com.quantexa.assignments.transactions.Service._


object TransactionsAssignment extends App {

  /***
    * A case class to represent a transaction
    */
  case class Transaction(
                          transactionId: String,
                          accountId: String,
                          transactionDay: Int,
                          category: String,
                          transactionAmount: Double)

  val inputFile = Source.fromInputStream(getClass.getResourceAsStream("/transactions.csv"))
  val transactionLines: Iterator[String] = inputFile.getLines().drop(1)
  inputFile.close()

  //splitting each line up by commas and construct Transactions
    val transactions: List[Transaction] = transactionLines.map { line =>
    val split = line.split(',')
    Transaction(split(0), split(1), split(2).toInt, split(3), split(4).toDouble)
    }.toList
  println("Question1 solution :- ")
  solutionQuestion1(transactions).foreach(println)
  println("\n\n\nQuestion2 solution :- ")
  solutionQuestion2(transactions).foreach(println)
  println("\n\n\nQuestion3 Solution :- ")
  solutionQuestion3(transactions).foreach(println)
}