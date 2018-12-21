package rdd

import base.Person
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * ${DESCRIPTION}
  *
  * @author yansen
  * @create 2018-12-17 18:07
  **/
object GetPartitionsTest {
  def main(args: Array[String]): Unit ={
    print("job start!")

    var conf = new SparkConf().setAppName("mysql").setMaster("local")
    var sc = new SparkContext(conf)
    var rdd = sc.parallelize(List(1,2,3,4,5,6)).map(_*3)

    var file1Rdd = rdd.map[String](x => {
      (Integer.parseInt(String.valueOf(x)) - 1 + 'a') + " " + x;
    });

    var fileRdd = sc.textFile("E:\\a.txt",3)

    var unionRdd = file1Rdd.union(fileRdd)

    unionRdd.foreach(println(_))

    println("untion")


    val visit = sc.parallelize(List(("index.html","1.2.3.4"),("about.html","3,4,5,6"),("index.html","1.3.3.1"),("hello.html","1,2,3,4")),2);
    val page = sc.parallelize(List(("index.html","home"),("about.html","about"),("hi.html","2.3.3.3")),2);

    visit.join(page)

    var i = 0;

    println(fileRdd.count())

    var persionRdd = fileRdd.map[Person](x =>{
      var strs:Array[String] = x.split(" ");

      new Person(strs(0),Integer.parseInt(strs(1)))
    });

//    persionRdd = persionRdd.sortBy(persion =>{
//      persion.age
//    },false)

    persionRdd.sortBy(_.age,true)

    persionRdd.foreach(person => println(person.name + person.age))

    println("sort")


    var groupRdd = fileRdd.groupBy[String]((x:String) =>{
      var s = x.split(" ");
      s(0);
    },4)


    var newGroupRdd = groupRdd.mapPartitionsWithIndex((x,iter) =>{
      var result = ListBuffer[String]()
      var i = 0
      while(iter.hasNext){
        var a = iter.next();

        result.+= (x + " " +a)
      }

      result.toList.iterator
    })

    newGroupRdd.foreach(println(_))

    println("group")


    fileRdd = fileRdd.mapPartitionsWithIndex(
      (x,iter) => {
        var result = ListBuffer[String]()
        var i = 0
        while(iter.hasNext){
          result.+=:(x + iter.next())
        }
        result.toList.iterator
      }
    )
    println("mapPartition");
    fileRdd.foreachPartition(partition =>{
      partition.foreach(a =>{
        println( a)
      })
    })

    println("foreachPartition" + fileRdd);


    val num = fileRdd.count()

    println(num);

    rdd.foreach( a => println(a))
    var mappedRdd = rdd.filter(_>10).collect();

    println(rdd.reduce(_+_))

    for(temp <- mappedRdd){
      println(temp + "")
    }

    print("")
    println("job success!")

  }
}


