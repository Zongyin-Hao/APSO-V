package solution

class Solution(n: Int, m: Int, d: Int, vms: Array[Array[Double]], pms: Array[Array[Double]], wd: Array[Double]) extends Serializable {

  private val assignment: Array[List[Int]] = new Array[List[Int]](m)
  private var assignmentNumber = 0
  private val usage: Array[Array[Double]] = Array.ofDim(m, d) // sum usage of each pm
  private var value = 1.0
  private val eps = 1e-8
  //initialize with constructor
  for (j <- 0 until m) {
    assignment(j) = List[Int]()
    for (k <- 0 until d) usage(j)(k) = 0.0
  }

  def debug(): Unit = {
    for (j <- 0 until m) {
      print("pm" + j + ":")
      for (i <- assignment(j)) print(" vm" + i)
      print(" ("); for (k <- 0 until d) print(" " + usage(j)(k).formatted("%.3f")); println(" )")
    }
    println("assignmentNumber = " + assignmentNumber)
  }

  def canAssigned(i: Int, j: Int): Boolean = {
    var flag = true
    for (k <- 0 until d if usage(j)(k) + vms(i)(k)/pms(j)(k) > 1.0) flag = false
    flag
  }

  def assign(i: Int, j: Int): Unit = {
    assignment(j) = i :: assignment(j)
    assignmentNumber += 1
    for (k <- 0 until d) usage(j)(k) += vms(i)(k)/pms(j)(k)
  }

  def calculate(): Unit = {
    if (assignmentNumber == n) {
      value = 0.0
      for (k <- 0 until d) {
        var avg = 0.0;
        for (j <- 0 until m) avg += usage(j)(k)
        avg /= m
        var st = 0.0
        for (j <- 0 until m) st += math.pow(usage(j)(k)-avg, 2)
        st = math.sqrt(st / m)
        value += wd(k)*st
      }
    }
  }

  def compareWith(that: Solution): Int = {
    if (math.abs(this.value-that.value) < eps) 0
    else if (this.value - that.value < 0) -1 else 1
  }

  def getAssignment: Array[List[Int]] = assignment

  def getUsage(j: Int, k: Int): Double = usage(j)(k)

  def getWUsage(j: Int): Double = {
    var wUsage = 0.0
    for (k <- 0 until d) wUsage += wd(k)*usage(j)(k)
    wUsage
  }

  def getValue: Double = value

}
