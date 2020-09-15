package pso

import org.apache.spark.SparkContext
import pso.particle._

import scala.util.Random

class APSOSpark(@transient val sc: SparkContext, n: Int, m: Int, d: Int,
                vms: Array[Array[Double]], pms: Array[Array[Double]], wd: Array[Double]) extends Serializable {

  private val splitNumber = 1
  private val communicationNumber = 5
  private val Iters = 100
  private val NP = 240
  private var w = 0.9
  private var c1 = 2.0
  private var c2 = 2.0
  private val xMin = -50.0
  private val xMax = 50.0
  private val vMin = -0.2*(xMax-xMin)
  private val vMax = 0.2*(xMax-xMin)
  private val state = (0.1, 0.15, 0.25)


  def start(): (Double, Int, Long) = {
    val startTime = System.currentTimeMillis()
    var (particles, optimalParticle) = initialize()

    for (_ <- 1 to communicationNumber) {
      val rdd = sc.parallelize(splitParticles(particles).map((_, optimalParticle)), splitNumber)
      particles = rdd.flatMap(pair => {
        val startTime_worker = System.currentTimeMillis()
        var particles_worker = pair._1
        var optimalParticle_worker = pair._2
        for (_ <- 1 to Iters/communicationNumber) {
          val f = calculateF(particles_worker, optimalParticle_worker)
          adjustParamter(f)
          if (f < state._1) optimalParticle_worker = jumpOut(optimalParticle_worker)
          particles_worker = particles_worker.map(nextStep(_, optimalParticle_worker))
          for (particle <- particles_worker if particle.getOptimalParticle().compareWith(optimalParticle_worker) < 0) {
            optimalParticle_worker = particle.getOptimalParticle()
          }
        }
        println("time = " + (System.currentTimeMillis() - startTime_worker) + ", Time" + (System.currentTimeMillis() - startTime))
        particles_worker
      }).collect()
      for (particle <- particles if particle.getOptimalParticle().compareWith(optimalParticle) < 0) {
        optimalParticle = particle.getOptimalParticle()
      }
    }

    (optimalParticle.getValue(), 1, System.currentTimeMillis() - startTime)
  }

  def initialize(): (Array[Particle], Particle) = {
    val particles = new Array[Particle](NP)
    var optimalParticle: Particle = null
    for (p <- 0 until NP) {
      particles(p) = new Particle(n, vMin, vMax, xMin, xMax)
      for (i <- 0 until n) {
        particles(p).setV(i, vMin + (vMax-vMin)*Random.nextDouble())
        particles(p).setX(i, xMin + (xMax-xMin)*Random.nextDouble())
      }
      particles(p).generate(null, m, d, vms, pms, wd)
      if (optimalParticle == null || particles(p).compareWith(optimalParticle) < 0) {
        optimalParticle = particles(p)
      }
    }
    (particles, optimalParticle)
  }

  def splitParticles(particles: Array[Particle]): Array[Array[Particle]] = {
    val blocks = new Array[Array[Particle]](splitNumber)
    val blockSize = math.ceil(1.0*particles.length / splitNumber).toInt
    for (k <- 0 until splitNumber) {
      blocks(k) = new Array[Particle](math.min(blockSize, particles.length-blockSize*k))
      for (idx <- blocks(k).indices) blocks(k)(idx) = particles(blockSize*k+idx)
    }
    blocks
  }

  def calculateF(particles: Array[Particle], optimalParticle: Particle): Double = {
    var dg = 0.0
    for (p <- particles.indices) dg += calculateDis(optimalParticle, particles(p))
    dg / (math.sqrt(n)*(xMax-xMin)*particles.length)
  }

  def calculateDis(p1: Particle, p2: Particle): Double = {
    var dis = 0.0
    for (i <- 0 until n) dis += math.pow(p1.getX(i)-p2.getX(i), 2)
    math.sqrt(dis)
  }

  def adjustParamter(f: Double): Unit = {
    if (f >= state._3) {
      w = 0.9
      c1 -= (Random.nextDouble()*0.05 + 0.05)
      c2 += (Random.nextDouble()*0.05 + 0.05)
    } else if (state._2 <= f && f < state._3) {
      w = (100*f+2)/30
      c1 += (Random.nextDouble()*0.05 + 0.05)
      c2 -= (Random.nextDouble()*0.05 + 0.05)
    } else if (state._1 <= f && f < state._2) {
      w = (100*f+2)/30
      c1 += (Random.nextDouble()*0.05 + 0.05)*0.5
      c2 -= (Random.nextDouble()*0.05 + 0.05)*0.5
    } else {
      w = 0.4
      c1 += (Random.nextDouble()*0.05 + 0.05)*0.5
      c2 += (Random.nextDouble()*0.05 + 0.05)*0.5
    }
    c1 = math.min(math.max(c1, 1.5), 2.5)
    c2 = math.min(math.max(c1, 1.5), 2.5)
    if (c1 + c2 > 4.0) {
      c1 = c1/(c1+c2)*4.0
      c2 = c2/(c1+c2)*4.0
    }
  }

  def jumpOut(preParticle: Particle): Particle = {
    val particle = new Particle(n, vMin, vMax, xMin, xMax)
    for (i <- 0 until n) {
      particle.setV(i, preParticle.getV(i))
      particle.setX(i, preParticle.getX(i))
    }
    val (randomIdx, flag) = (Random.nextInt(n), Random.nextBoolean())
    val x = particle.getX(randomIdx)
    if (flag) particle.setX(randomIdx, x + (xMax-x)*Random.nextDouble())
    else particle.setX(randomIdx, x - (x-xMin)*Random.nextDouble())
    particle.generate(preParticle, m, d, vms, pms, wd)
    if (particle.compareWith(preParticle) < 0) particle else preParticle
  }

  def nextStep(preParticle: Particle, optimalParticle: Particle): Particle = {
    val particle = new Particle(n, vMin, vMax, xMin, xMax)
    for (i <- 0 until n) {
      particle.setV(i, w * preParticle.getV(i) +
        c1 * Random.nextDouble() * (preParticle.getOptimalParticle.getX(i) - preParticle.getX(i)) +
        c2 * Random.nextDouble() * (optimalParticle.getX(i) - preParticle.getX(i)))
      particle.setX(i, preParticle.getX(i) + particle.getV(i))
    }
    particle.generate(preParticle, m, d, vms, pms, wd)
    particle
  }

  def debug(): Unit = {
    System.exit(0)
  }

}