package heuristic

import solution.Solution

class BF(n: Int, m: Int, d: Int, vms: Array[Array[Double]], pms: Array[Array[Double]], wd: Array[Double]) {

  def start(): (Double, Int, Long) = {
    val startTime = System.currentTimeMillis()
    def getWResource: Int=>Double = (i: Int)=>{
      var wResource = 0.0
      for (k <- 0 until d) wResource += wd(k)*vms(i)(k)/pms(0)(k) // /pms(0)(k) is to normalize each dimension
      wResource
    }
    val solution = generateSolutionByBF((0 until n).toArray)
//    val solution = generateSolutionByBF((0 until n).toArray.sortWith((i1, i2) => getWResource(i1) > getWResource(i2)))
    (solution.getValue, 1, System.currentTimeMillis()-startTime)
  }

  def generateSolutionByBF(vmseq: Array[Int]): Solution = {
    val solution = new Solution(n, m, d, vms, pms, wd)
    vmseq.foreach(i => {
      var bestJ = -1
      for (j <- 0 until m if solution.canAssigned(i, j) &&
        (bestJ == -1 || solution.getWUsage(j) < solution.getWUsage(bestJ))) bestJ = j
      if (bestJ >= 0) solution.assign(i, bestJ)
    })
    solution.calculate()
    solution
  }

}
