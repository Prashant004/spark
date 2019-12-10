package com.prashant.spark


import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object FriendsCount {

  def parseLine(line: String) = {

    val fields = line.split(",")

    val fname = fields(1)
    val age = fields(2)
    val friendsCount = fields(3).toInt

    //create tuple
    (age, friendsCount)

  }

  def main(args: Array[String]): Unit = {

     Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]", "FriendsCount")
    val lines = sc.textFile("../resources/fakefriends.csv")

    val rdd = lines.map(parseLine)
    println("Total number of entries ", rdd.count())
    println("Total number of unique entries ", rdd.distinct())

   val totalsByAge = rdd.mapValues(x => (x, 1)).reduceByKey( (x,y) => (x._1 + y._1, x._2 + y._2))
   
   val averageByName = totalsByAge.mapValues(x => x._1/x._2)
   val results = averageByName.collect()
   
   results.sorted.foreach(println)

  }

}