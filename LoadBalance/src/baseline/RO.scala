package baseline

import scala.util.Random

import solution.Solution

class RO(n: Int, m: Int, d: Int, vms: Array[Array[Double]], pms: Array[Array[Double]], wd: Array[Double]) {

  val Iters = 2500
  val random = new Random()

  def start(): (Double, Int, Long) = {
    var optimalSolution: Solution = null
    var convergence = 1
    val startTime = System.currentTimeMillis()

    for (iter <- 1 to Iters) {
      val solution = generateRandomSolution()
      if (optimalSolution == null || solution.compareWith(optimalSolution) < 0) {
        optimalSolution = solution
        convergence = iter
      }
    }

    (optimalSolution.getValue, convergence, System.currentTimeMillis() - startTime)
  }

  def generateRandomSolution(): Solution = {
    val solution = new Solution(n, m, d, vms, pms, wd)
    val vmseq = generateRandomSequence(n)
    for (i <- vmseq) {
      var candidateList = List[Int]()
      for (j <- 0 until m if solution.canAssigned(i, j)) candidateList = j::candidateList
      val candidateArr = candidateList.toArray
      if (!candidateArr.isEmpty) {
        val randomIndex = random.nextInt(candidateArr.length)
        solution.assign(i, candidateArr(randomIndex))
      }
    }
    solution.calculate()
    solution
  }

  def generateRandomSequence(len: Int): Array[Int] = {
    val seq = Array.range(0, len)
    for (i <- 0 until len) {
      val j = random.nextInt(len)
      val t = seq(i); seq(i) = seq(j); seq(j) = t
    }
    seq
  }

  def debug(): Unit = {
    System.exit(0)
  }

}
