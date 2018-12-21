package rdd

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * ${DESCRIPTION}
  *
  * @author yansen
  * @create 2018-12-19 17:59
  **/
object WordCount {
  def main(args: Array[String]): Unit ={
    var conf = new SparkConf().setAppName("test").setMaster("local")
    var sc = new SparkContext(conf)

    var fileRdd = sc.textFile("E:\\b.txt");

    var file1Rdd = sc.textFile("E:\\c.txt");

    var result = fileRdd.flatMap(_.split(" ")).map((_,1))

    var result2 = file1Rdd.flatMap(_.split(" ")).map((_,1))

    var joinRdd = result2.join[Int](result)

     var result3 =  joinRdd.flatMap[(String,Int)](x => {
      var key = x._1;
      var value = x._2._1 + x._2._1;

      var result = new Array[(String,Int)](1);

      result(0) = (key,value);
      result
    }).reduceByKey(_ + _)

    result3.sortBy(_._2).map[(Int,String)](x => (x._2,x._1)).top(5).foreach(
      x => println("(" + x._2 + "," + x._1 + ")")
    );

    result3.sortBy(_._2).map[(Int,String)](x => (x._2,x._1)).take(5).foreach(
      x => println("(" + x._2 + "," + x._1 + ")")
    );

    println("top5")

    var result1 = fileRdd.flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _).lookup("the")

    result.reduceByKey(_+_)

    result.foreach(println(_))

    println("lookup")

    result1.foreach(println(_))
  }

}
