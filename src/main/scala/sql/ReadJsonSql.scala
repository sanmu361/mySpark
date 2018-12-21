package sql

import base.GDP
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

import scala.io.Source
import scala.util.parsing.json.JSON

/**
  * ${DESCRIPTION}
  *
  * @author yansen
  * @create 2018-12-21 17:45
  **/
object ReadJsonSql {
  def main(args: Array[String]): Unit = {

    val schema = StructType(
      Seq(
        StructField("avg_gdp",DoubleType,true)
        ,StructField("avg_per",DoubleType,true)
        ,StructField("id",IntegerType,true)
        ,StructField("region_name",StringType,true)
      )
    )

    var conf = new SparkConf().setAppName("JsonTest").setMaster("local")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val sparkSession = SparkSession.builder().appName("mysql").config(conf).getOrCreate()

    var files = sc.textFile("E:\\gdp.txt").map(pair => new String(pair.getBytes, 0, pair.length, "GBK"))

//    var fileJson = sqlContext.createDataFrame(files,schema);

//    var fileJson = sqlContext.read.json(files);

    files.saveAsTextFile("E:\\gdp2")

//    fileJson.show()

//    var info = sparkSession.read.json("E:\\gdp.txt")
//
//    info.foreach(x =>{
////      println(x)
//      if(x.get(3) != null){
//        println(x.get(3).getClass.getName)
//      }
//    })
//
//    info.map(x => {
//      new GDP(x.get(0).asInstanceOf[Double].toDouble,x.get(1).asInstanceOf[Double].toDouble,x.get(2).asInstanceOf[Double].toDouble,x.get(3).asInstanceOf[Int].toInt,x.get(0).asInstanceOf[String].toString)
//    })

//    info.show()

//    var info = sqlContext.read.json("E:\\gdp.txt","GBK")
//
//    info.toJSON.rdd.saveAsTextFile("E:\\gdp1.txt")
//
//    info.show()
//
//    println(info.getClass.getName)
//
//    println(info.schema)

  }
}
