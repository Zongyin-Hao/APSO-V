package main

import java.io.{File, PrintWriter}

import scala.io.Source
import baseline.RO
import aco.SACO
import heuristic.BF
import pso.{APSO, GPSO}

object Main {
  val loop = 10 // test each sample loop times
  val d = 2 // resource dimension, put cpu information in the first dimension
  val wd = new Array[Double](d)
  wd(0) = 0.5
  wd(1) = 0.5

  def main(args: Array[String]): Unit = {
    debug("APSO", "testcaseec2/400")
//    test("APSO", "testcaseec2", "output")
  }

  def debug(algorithm: String, input: String): Unit = {
    val (n, m, vms, pms) = readFile(input)
    val (value, convergenceTimes, time) = startAlgorithm(algorithm, n, m, vms, pms)
    println("value = " + value + ", convergenceTimes = " + convergenceTimes + ", time = " + time)
  }

  // start testing
  def test(algorithm: String, inputDir: String, outputPath: String): Unit = {
    val output = new PrintWriter(outputPath)
    new File(inputDir).listFiles().foreach(file => {
      val (n, m, vms, pms) = readFile(file.getPath)
      var (minValue, avgValue, avgConvergenceTimes, avgTime) = (1.0, 0.0, 0.0, 0.0)

      for (_ <- 0 until loop) {
        val (value, convergenceTimes, time) = startAlgorithm(algorithm, n, m, vms, pms)
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
  def startAlgorithm(algorithm: String, n: Int, m: Int, vms: Array[Array[Double]], pms: Array[Array[Double]]): (Double, Int, Long) = {
    if (algorithm.equals("BF")) new BF(n, m, d, vms, pms, wd).start()
    else if (algorithm.equals("SACO")) new SACO(n, m, d, vms, pms, wd).start()
    else if (algorithm.equals("GPSO")) new GPSO(n, m, d, vms, pms, wd).start()
    else if (algorithm.equals("APSO")) new APSO(n, m, d, vms, pms, wd).start()
    else new RO(n, m, d, vms, pms, wd).start()
  }

}
