def findX(a: Double, radius: Int): Int = {
  val radian = Math.toRadians(a)

  (radius * math.cos(radian)).intValue
}

def findY(a: Double, radius: Int): Int = {
  val radian = Math.toRadians(a)
  (radius * math.sin(radian)).intValue
}

var op = 90000
val speed = 2
var cur = 0
while (true) {
  cur = cur + speed
  val angle: Double = (360 / op.toDouble) * cur
  println(angle)
  val x = findX(angle, 10000)
  val y = findY(angle, 10000)
  println(s"($x, $y)")

  Thread.sleep(100)
}

println("hi")