package com.prashant.spark

import org.apache.spark.SparkContext
import scala.math.max
import org.apache.log4j._

object IndiaMaxWeather {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "India_Weather");

    val lines = sc.textFile("../resources/India.csv");

    val parsedLines = lines.map(parseLine)

    val yearWiseMeanTemp = parsedLines.map((x) => (x._1, x._2))

    val maxTemp = yearWiseMeanTemp.reduceByKey((x, y) => max(x, y))

    val results = maxTemp.collect();

    for (result <- results.sorted) {
      val year = result._1;
      val temp = result._2;
      val formattedTemp = f"$temp%.2f C";

      println(s"Max annual Avg Temp -> year $year is ----> $formattedTemp");
    }

  }

  def parseLine(lines: String) = {

    val fields = lines.split(",");

    val year = fields(0).toInt;
    val annualMeanTemp = fields(1).toFloat;

    (year, annualMeanTemp)
  }
}