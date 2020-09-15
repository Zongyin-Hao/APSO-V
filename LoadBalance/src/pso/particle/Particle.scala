package pso.particle

import solution._

import scala.util.control.Breaks._

class Particle(n: Int, vMin: Double, vMax: Double, xMin: Double, xMax: Double) extends Serializable {

  private val v = new Array[Double](n)
  private val x = new Array[Double](n)
  private var value = 1.0
  private val eps = 1e-8
  private var optimalParticle = this

  // BF
  def generate(preParticle: Particle, m: Int, d:Int, vms: Array[Array[Double]], pms: Array[Array[Double]], wd: Array[Double]): Unit = {
    val solution = new Solution(n, m, d, vms, pms, wd)
    (0 until n).toArray.sortWith((i1, i2) => x(i1) < x(i2)).foreach(i => breakable {
      var bestJ = -1
      for (j <- 0 until m if solution.canAssigned(i, j) &&
        (bestJ == -1 || solution.getWUsage(j) < solution.getWUsage(bestJ))) bestJ = j
      if (bestJ >= 0) solution.assign(i, bestJ)
    })
    solution.calculate()
    value = solution.getValue
    if (preParticle != null && preParticle.optimalParticle.compareWith(optimalParticle) < 0)
      optimalParticle = preParticle.optimalParticle
  }

  def compareWith(that: Particle): Int = {
    if (math.abs(this.value-that.value) < eps) 0
    else if (this.value - that.value < 0) -1 else 1
  }

  def setV(index: Int, value: Double): Unit = {
    v(index) = math.min(math.max(value, vMin), vMax)
  }

  def setX(index: Int, value: Double): Unit = {
    x(index) = math.min(math.max(value, xMin), xMax)
  }

  def getV(index: Int): Double = v(index)

  def getX(index: Int): Double = x(index)

  def getValue(): Double = value

  def getOptimalParticle(): Particle = optimalParticle

}
