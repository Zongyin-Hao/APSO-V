package aco

import solution.Solution

import scala.util.Random
import scala.util.control.Breaks._

class SACO(n: Int, m: Int, d: Int, vms: Array[Array[Double]], pms: Array[Array[Double]], wd: Array[Double]) {

  private val Iters = 100
  private val AN = 25
  private val alpha = 1.0
  private val beta = 5.0
  private val p = 0.5
  private val p0 = 1.0
  private val random = new Random()
  private val phe: Array[Array[Double]] = Array.ofDim[Double](n, m)
  for (i <- 0 until n; j <- 0 until m) phe(i)(j) = p0

  def start(): (Double, Int, Long) = {
    val startTime = System.currentTimeMillis()
    var convergence = 1
    def getWResource: Int=>Double = (i: Int)=>{
      var wResource = 0.0
      for (k <- 0 until d) wResource += wd(k)*vms(i)(k)/pms(0)(k) // /pms(0)(k) is to normalize each dimension
      wResource
    }
    var optimalSolution = generateRandomSolution()

    for (iter <- 1 to Iters) {
      for (_ <- 1 to AN) {
        val solution = generateSolution()
        if (solution.compareWith(optimalSolution) < 0) {
          convergence = iter
          optimalSolution = solution
        }
      }

      val assignment = optimalSolution.getAssignment
      for (j <- assignment.indices) {
        for (i <- assignment(j)) {
          phe(i)(j) = (1-p)*phe(i)(j)+1/optimalSolution.getValue
        }
      }
    }

    (optimalSolution.getValue, convergence, System.currentTimeMillis() - startTime)
  }

  def debug(): Unit = {
    System.exit(0)
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

  def generateSolution(): Solution = {
    val solution = new Solution(n, m, d, vms, pms, wd)
    val phe_heu: (Int, Int)=>Double = (i: Int, j: Int)=>{
      math.pow(phe(i)(j),alpha)*math.pow(1-solution.getWUsage(j),beta)
    }
    breakable { for (i <- 0 until n) {
      // get candidate physical machines
      val candidate = for (j <- 0 until m if solution.canAssigned(i, j)) yield j
      if (candidate.isEmpty) break // assign failed
      // roulette wheel
      var sum = 0.0
      for (j <- candidate) sum += phe_heu(i, j)
      val q = random.nextDouble() * sum
      var (curJ, curSum) = (-1, 0.0)
      for (j <- candidate if curSum <= q) {
        curJ = j
        curSum += phe_heu(i, j)
      }
      assert(curJ >= 0)
      solution.assign(i, curJ)
    } }
    solution.calculate()
    solution
  }

}
