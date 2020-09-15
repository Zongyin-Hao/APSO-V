package main

import java.io.{File, PrintWriter}

import scala.io.Source
import pso.APSOSpark
import org.apache.spark.{SparkConf, SparkContext}

object MainSpark {
  val loop = 10 // test each sample loop times
  val d = 2 // resource dimension, put cpu information in the first dimension
  val wd = new Array[Double](d)
  wd(0) = 0.5
  wd(1) = 0.5

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
//    conf.setAppName("Main").setMaster("local")
    val sc = new SparkContext(conf)
    if (args(0).equals("signal")) test_signal(sc, args(1), args(2))
    else test(sc, args(1), args(2), args(3))
  }

  def test_signal(sc: SparkContext, algorithm: String, input: String): Unit = {
    val (n, m, vms, pms) = readFile(input)
    val (value, convergenceTimes, time) = startAlgorithm(sc, algorithm, n, m, vms, pms)
    println("value = " + value + ", convergenceTimes = " + convergenceTimes + ", time = " + time)
  }

  // start testing
  def test(sc: SparkContext, algorithm: String, inputDir: String, outputPath: String): Unit = {
    val output = new PrintWriter(outputPath)
    new File(inputDir).listFiles().foreach(file => {
      val (n, m, vms, pms) = readFile(file.getPath)
      var (minValue, avgValue, avgConvergenceTimes, avgTime) = (1.0, 0.0, 0.0, 0.0)

      for (_ <- 0 until loop) {
        val (value, convergenceTimes, time) = startAlgorithm(sc, algorithm, n, m, vms, pms)
        minValue = math.min(minValue, value); avgValue += value
        avgConvergenceTimes += convergenceTimes; avgTime += time
      }

      println(file.getName +
        "\t" + minValue.formatted("%8.3f") + "\t" + (avgValue/loop).formatted("%8.3f") +
        "\t" + (avgConvergenceTimes/loop).formatted("%8.3f") + "\t" + (avgTime/loop).formatted("%8.3f"))
      output.println(file.getName +
        "\t" + minValue.formatted("%8.3f") + "\t" + (avgValue/loop).formatted("%8.3f") +
        "\t" + (avgConvergenceTimes/loop).formatted("%8.3f") + "\t" + (avgTime/loop).formatted("%8.3f"))
    })
    output.close()
  }

  // read n, m, vms(n, d), pms(m, d)
  def readFile(path: String): (Int, Int, Array[Array[Double]], Array[Array[Double]]) = {
    val source = Source.fromFile(path)
    val lines = source.getLines()
    // read n, m
    val firstLine = lines.next().split(" ")
    val (n, m) = (firstLine(0).toInt, firstLine(1).toInt)
    // read vms(n, d)
    val vms = Array.ofDim[Double](n, d)
    for (i <- 0 until n) {
      val line = lines.next().split(" ")
      for (k <- 0 until d) vms(i)(k) = line(k).toDouble
    }
    // read pms(m, d)
    val pms = Array.ofDim[Double](m, d)
    for (j <- 0 until m) {
      val line = lines.next().split(" ")
      for (k <- 0 until d) pms(j)(k) = line(k).toDouble
    }
    source.close()
    (n, m, vms, pms)
  }

  // choose algorithm
  def startAlgorithm(sc: SparkContext, algorithm: String, n: Int, m: Int, vms: Array[Array[Double]], pms: Array[Array[Double]]): (Double, Int, Long) = {
    if (algorithm.equals("APSOSpark")) new APSOSpark(sc, n, m, d, vms, pms, wd).start()
    else (-1.00, -1, -1)
  }

}
