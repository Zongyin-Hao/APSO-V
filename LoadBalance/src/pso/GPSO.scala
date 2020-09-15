package pso

import pso.particle._

import scala.util.Random

class GPSO(n: Int, m: Int, d: Int, vms: Array[Array[Double]], pms: Array[Array[Double]], wd: Array[Double]) {

  private val Iters = 100
  private val NP = 25
  private val wInit = 0.9
  private val wEnd = 0.4
  private var w = wInit
  private val c1 = 2.0
  private val c2 = 2.0
  private val xMin = -50.0
  private val xMax = 50.0
  private val vMin = -0.2*(xMax-xMin)
  private val vMax = 0.2*(xMax-xMin)
  private val random = new Random()

  def start(): (Double, Int, Long) = {
    val startTime = System.currentTimeMillis()
    var convergence = 1
    var (particles, optimalParticle) = initialize()
    for (iter <- 1 to Iters) {
      particles = particles.map(nextStep(_, optimalParticle))
      for (particle <- particles if particle.compareWith(optimalParticle) < 0) {
        optimalParticle = particle
        convergence = iter
      }
      w = (wInit-wEnd)*(Iters-iter)/Iters + wEnd
    }
    (optimalParticle.getValue(), convergence, System.currentTimeMillis() - startTime)
  }

  def initialize(): (Array[Particle], Particle) = {
    val particles = new Array[Particle](NP)
    var optimalParticle: Particle = null
    for (p <- 0 until NP) {
      particles(p) = new Particle(n, vMin, vMax, xMin, xMax)
      for (i <- 0 until n) {
        particles(p).setV(i, vMin + (vMax-vMin)*random.nextDouble())
        particles(p).setX(i, xMin + (xMax-xMin)*random.nextDouble())
      }
      particles(p).generate(null, m, d, vms, pms, wd)
      if (optimalParticle == null || particles(p).compareWith(optimalParticle) < 0) {
        optimalParticle = particles(p)
      }
    }
    (particles, optimalParticle)
  }

//  def calculateF(particles: Array[Particle], optimalParticle: Particle): Double = {
//    var dg = 0.0
//    for (p <- 0 until NP) dg += calculateDis(optimalParticle, particles(p))
//    dg / (math.sqrt(n)*(xMax-xMin)*NP)
//  }
//
//  def calculateDis(p1: Particle, p2: Particle): Double = {
//    var dis = 0.0
//    for (i <- 0 until n) dis += math.pow(p1.getX(i)-p2.getX(i), 2)
//    math.sqrt(dis)
//  }

  def nextStep(preParticle: Particle, optimalParticle: Particle): Particle = {
    val particle = new Particle(n, vMin, vMax, xMin, xMax)
    for (i <- 0 until n) {
      particle.setV(i, w * preParticle.getV(i) +
        c1 * random.nextDouble() * (preParticle.getOptimalParticle.getX(i) - preParticle.getX(i)) +
        c2 * random.nextDouble() * (optimalParticle.getX(i) - preParticle.getX(i)))
      particle.setX(i, preParticle.getX(i) + particle.getV(i))
    }
    particle.generate(preParticle, m, d, vms, pms, wd)
    particle
  }

  def debug(): Unit = {
    System.exit(0)
  }

}