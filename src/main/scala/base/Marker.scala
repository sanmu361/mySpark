package base

/**
  * ${DESCRIPTION}
  *
  * @author yansen
  * @create 2018-12-18 18:38
  **/
class Marker private(val color:String) {

  println("创建" + this)

  override def toString(): String = "颜色标记："+ color

}

object Marker{

  private val markers: Map[String, Marker] = Map(
    "red" -> new Marker("red"),
    "blue" -> new Marker("blue"),
    "green" -> new Marker("green")
  )

  def apply(color:String) = {
    if(markers.contains(color)) markers(color) else null
  }


  def getMarker(color:String) = {
    if(markers.contains(color)) markers(color) else null
  }

}
