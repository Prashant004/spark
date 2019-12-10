package com.prashant.spark

import org.apache.log4j._
import org.apache.spark.SparkContext

object CustomerExpenses {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "CustomerExpenses");

    val lines = sc.textFile("../resources/customerOrders.csv");

    val input = lines.map(parseline);

    val eachCustomer = input.map((x) => (x._1, x._2)).reduceByKey((x, y) => (x + y))
    
    val highestSpender = eachCustomer.map((x) => (x._2,x._1))
    
    val newResult = highestSpender.collect();

    val results = eachCustomer.collect();

    for (result <- results.sorted) {

      val customerId = result._1
      val amount = result._2
      val formattedAmount = f"$amount%.2f"

//      println(s"ID $customerId spent $formattedAmount dollars")
    }
    
    //print expenses by customer in ascending order
    newResult.sorted.foreach(println);

  }

  def parseline(lines: String) = {
    val fields = lines.split(",")

    val customerId = fields(0).toInt;
    val amount = fields(2).toFloat;

    (customerId, amount)
  }
}