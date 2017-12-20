
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * ${DESCRIPTION}
  *
  * @author yansen
  * @create 2017-11-20 20:03
  **/

object HdfsJob {

  def main(args: Array[String]): Unit ={
    print("job start!")

    val schema = StructType(
      Seq(
        StructField("name",StringType,true)
        ,StructField("age",IntegerType,true)
      )
    )

    var conf = new SparkConf().setMaster("local[1]")
    var sc = new SparkContext(conf)

    val sparkSession = SparkSession.builder().appName("mysql").config(conf).getOrCreate()

    val textFileRdd = sc.textFile("/sanmu/mapreduce/input/a.txt").map(_.split(" ")).map(p => Row(p(0),p(1).toInt))

    var dataFrame = sparkSession.createDataFrame(textFileRdd,schema)

    dataFrame.write.mode("overwrite").saveAsTable("table1")


    var rdd = sparkSession.sql("select * from table1 where age > 30")


    rdd.show()
  }

}


