package sql

import java.io.File
import java.nio.charset.Charset
import java.util
import java.util.{ArrayList, List}

import base.GDP
import com.google.common.io.Files
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}

import scala.beans.BeanProperty



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

//    case class GDP(){
//
//      @BeanProperty var avg_gdp:Double = _
//      @BeanProperty var avg_per:Double = _
//      @BeanProperty var id:Int = _
//      @BeanProperty var region_name:String = _
//    }

//    case class NestedStruct(f: GDP)

    var conf = new SparkConf().setAppName("JsonTest").setMaster("local")
    val sc = new SparkContext(conf)

//    val sqlContext = new SQLContext(sc)

    val sparkSession = SparkSession.builder().appName("mysql").config(conf).getOrCreate()

    val newFile: File = new File("E:\\gdp.txt")
    val list: util.List[String] = Files.readLines(newFile, Charset.forName("GB2312"))


    val gson = new Gson
    val l:util.ArrayList[GDP] = new ArrayList[GDP]();

    import scala.collection.JavaConversions._
    for (str <- list) {
       l.addAll(gson.fromJson(str, new TypeToken[util.ArrayList[GDP]]() {}.getType))
    }

    var rdd = sc.parallelize(l).map(p => Row(p.avg_per.toDouble,p.avg_gdp.toDouble,p.id.toInt,p.region_name))

    var rdd1 = sc.parallelize(l).map(p => new GDP(p.avg_per.toDouble,p.avg_gdp.toDouble,p.id.toInt,p.region_name))

    implicit val myObjectEncoder = org.apache.spark.sql.Encoders.kryo[GDP]

    var dataSet1 = sparkSession.createDataset(rdd1)
//      dataSet1.foreach(g =>{
//      if(g.region_name != null){
//        println(g.region_name)
//      }
//    })

    implicit val myObjectEncoder1 = org.apache.spark.sql.Encoders.kryo[Row]

    var df1 = dataSet1.map(p =>{
      Row(p.avg_per.toDouble,p.avg_gdp.toDouble,p.id.toInt,p.region_name)
    })

    df1.show()


//      df1.as[GDP].foreach(g =>{
//      println(g.id + " 1  "+ g.region_name)
//    })





    var test = sparkSession.createDataFrame(rdd,schema)

    test.createTempView("t_table1")

    var dfResult = sparkSession.sql("select * from t_table1 where id < 255")

    dfResult.show()

    var testDS = test.map(r =>{
      new GDP(r.getDouble(0),r.getDouble(1),r.getInt(2),r.getString(3))
    })
      testDS.foreach(g =>{
            println(g.id + " 2  "+ g.region_name)
          })

//    testDS.createTempView("t_table2")
//
//    sparkSession.sql("select * from t_table2 where id > 2000").show()
//    for (m <- l) {
//      println(m.avg_gdp)
//    }

//    implicit val myObjectEncoder = org.apache.spark.sql.Encoders.kryo[GDP]
////    import sparkSession.implicits._
//    var a = sparkSession.read.json("E:\\gdp_test.txt")
//
//    a.as[GDP].show(1)




//    sparkSession.read.json()


//    var files = sc.textFile("E:\\gdp.txt").
//      .map(pair => new String(pair.getBytes, 0, pair.length, "GB2312"))
//      .flatMap(x =>{
//        println(x)
//        var arr = JSON.parseFull(x)
//        arr.toList
//      })

//    var fileJson = sqlContext.createDataFrame(files,schema);

//    var fileJson = sqlContext.read.json(files);


    import sparkSession.implicits._



//    files.saveAsTextFile("E:\\gdp2")

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
