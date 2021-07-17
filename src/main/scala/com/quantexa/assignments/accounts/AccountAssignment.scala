package com.quantexa.assignments.accounts

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/***
 * Problem Statement
 * Collate all accounts information for each customer, highlighting total and average balance amount.
 */
object AccountAssignment extends App {

  //Create a spark context, using a local master so Spark runs on the local machine
  val spark = SparkSession.builder().master("local[*]").appName("AccountAssignment").getOrCreate()

  //importing spark implicits allows functions such as dataframe.as[T]
  import spark.implicits._

  //Set logger level to Warn
  Logger.getRootLogger.setLevel(Level.WARN)
  
  //Get file paths from files stored in project resources
  val customerCSV = getClass.getResource("/customer_data.csv").getPath
  val accountCSV = getClass.getResource("/account_data.csv").getPath

  //Create DataFrames of sources
  val customerDF = spark.read.option("header","true")
    .csv(customerCSV)
  val accountDF = spark.read.option("header","true")
    .csv(accountCSV)

  case class CustomerData(
                          customerId: String,
                          forename: String,
                          surname: String
                        )

  case class AccountData(
                          customerId: String,
                          accountId: String,
                          balance: Long
                        )

  //Output Format
  case class CustomerAccountOutput(
                                    customerId: String,
                                    forename: String,
                                    surname: String,
                                    accounts: Seq[AccountData],
                                    numberAccounts: Int,
                                    totalBalance: Long,
                                    averageBalance: Double
                                  )

  //Create Datasets of sources
  val customerDS = customerDF.as[CustomerData]
  val accountDS = accountDF.withColumn("balance",'balance.cast("long")).as[AccountData]

 //Convert accountDS according to output
  val accountOutput=accountDS.groupBy("customerId")
                    .agg(collect_set(struct("customerId","accountId","balance")) as "accounts",
                      count("accountId").cast("Int") as "numberAccounts",
                      sum("balance").cast("long")as "totalBalance",
                      avg("balance").cast("double")as "averageBalance")
  //Joining customerData and accountData
  val customerAccountOutputDS=customerDS.join(accountOutput,Seq("customerId"),"left").na.fill(0)
                        .sort(customerDS("customerId")).as[CustomerAccountOutput]

  //Displaying output
  customerAccountOutputDS.show(false)

}
