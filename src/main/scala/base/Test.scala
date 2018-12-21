package base

/**
  * ${DESCRIPTION}
  *
  * @author yansen
  * @create 2018-12-18 18:39
  **/
object Test {
  def main(args: Array[String]) {
    println(Marker("red"))
    // 单例函数调用，省略了.(点)符号
    println(Marker getMarker "blue")
  }
}
