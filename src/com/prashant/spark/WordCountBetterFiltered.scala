package com.prashant.spark

import org.apache.spark.SparkContext
import org.apache.log4j._
object WordCountBetterFiltered {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "WordCountFiltered");

    val input = sc.textFile("../resources/book.txt");

    val words = input.flatMap(x => x.split("\\W+"))

    val lowerCaseWords = words.map(x => x.toLowerCase());

    val countOfWords = lowerCaseWords.map(x => (x, 1)).reduceByKey((x, y) => x + y);

    val sortedByCount = countOfWords.map(x => (x._2, x._1)).sortByKey()

    val filteredWords = sortedByCount.filter(x => !(x._2.equals("is") | x._2.equals("are") | x._2.equals("a") | x._2.equals("the") | x._2.equals("if")));

    //     val filteredWords = sortedByCount.filter(x => !(x._2 =="is"));

    val results = filteredWords.collect();

    for (result <- results) {
      val word = result._2
      val count = result._1

      println(s"$word: $count")
    }

  }
}