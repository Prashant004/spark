package com.prashant.spark

import org.apache.spark.SparkContext._
import org.apache.spark._
import scala.math.max
import org.apache.log4j._

object MaxTemperatures {

  def parseLine(lines: String) = {
    val fields = lines.split(",");

    val stationId = fields(0);
    val entryType = fields(2);
    val temperature = fields(3)
    val tempInCelcius = ((temperature.toFloat)-32.0f)*(5.0f/9.0f);

    (stationId, entryType, tempInCelcius);
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "MaxTemperatures")

    val lines = sc.textFile("../resources/1800.csv")

    val parsedLines = lines.map(parseLine);

    val maxTemps = parsedLines.filter(x => x._2 == "TMAX")

    val stationTemps = maxTemps.map(x => (x._1, x._3.toFloat))

    val maxTempByStation = stationTemps.reduceByKey((x, y) => max(x, y))
//    val maxTempByStation = stationTemps.reduce((x, y) =>(y))
//    println(maxTempByStation)

    val result = maxTempByStation.collect();

    for (stationTemp <- result.sorted) {
      val station = stationTemp._1
      val temp = stationTemp._2
      val formattedTemp = f"$temp%.2f C"
      println(s"$station   Maximum Temperature:   $formattedTemp")
    }

  }

}