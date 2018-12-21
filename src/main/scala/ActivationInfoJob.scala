import org.apache.spark.{SparkConf, SparkContext}

/**
  * ${DESCRIPTION}
  *
  * @author yansen
  * @create 2017-11-13 10:59
  **/
object ActivationInfoJob {
  def main(args: Array[String]): Unit ={
    print("job start!")

    var conf = new SparkConf().setAppName("mysql").setMaster("local")
    var sc = new SparkContext(conf)
    var rdd = sc.parallelize(List(1,2,3,4,5,6)).map(_*3)

    rdd.foreach( a => println(a))
    var mappedRdd = rdd.filter(_>10).collect()

    println(rdd.reduce(_+_))

    for(temp <- mappedRdd){
      println(temp + "")
    }

    print("")
    println("job success!")

  }
}
