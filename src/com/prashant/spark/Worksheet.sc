package com.pra.spark

import org.apache.log4j._
import org.apache.spark.SparkContext

object CustomerExpenses {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "CustomerExpenses");

    val lines = sc.textFile("../customerOrders.csv");

    val input = lines.map(parseline);

    val eachCustomer = input.map((x) => (x._1, x._2)).reduceByKey((x, y) => (x + y))

    val results = eachCustomer.collect();

    for (result <- results.sorted) {

      val customerId = result._1
      val amount = result._2
      val formattedAmount = f"$amount%.2f"

      println(s"ID $customerId spent $formattedAmount dollars")
    }

  }

  
}