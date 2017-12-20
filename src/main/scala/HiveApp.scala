import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.hive.HiveContext

/**
  * ${DESCRIPTION}
  *
  * @author yansen
  * @create 2017-11-14 11:11
  **/
object HiveApp {

  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setAppName("hiveApp")

    val sc = new SparkContext(conf)

    val spark = SparkSession.builder().appName("hiveApp").config(conf).enableHiveSupport().getOrCreate()

//    var mysqlDF = mysqlData(spark)
    val hiveData = new HiveData();
    hiveData.hiveData()

    var hiveDF = hiveData1(spark,conf)

//    var resultDF = mysqlDF.join(hiveDF,mysqlDF("id").equalTo(hiveDF("area_id"))).select("name","center_scope","center_scope_name","area_id","scope","scope_name")
//
//    resultDF.write.mode(SaveMode.Append)
//      .format("jdbc")
//      .option("url", "jdbc:mysql://192.168.181.131:3306/result_test?useUnicode=true&characterEncoding=utf-8")
//      .option("dbtable", "test")
//      .option("diver","com.mysql.jdbc.Driver")
//      .option("user","root")
//      .option("password","12345")
//      .save()
  }

  private def mysqlData(spark:SparkSession): DataFrame ={
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://10.237.14.235:3306/xm_ci_zg?useUnicode=true&characterEncoding=utf-8")
      .option("dbtable", "area")
      .option("diver","com.mysql.jdbc.Driver")
      .option("user","zg_root")
      .option("password","7658032")
      .load()
    //虚拟表
    jdbcDF.select("id", "name", "center_scope","center_scope_name").write.mode("overwrite").saveAsTable("user_abel")
    val jdbcRDD = spark.sql("select * from user_abel")
    println("mysql 表")
    jdbcRDD.show()
    jdbcRDD
  }

  private def hiveData(spark:SparkSession,conf:SparkConf): DataFrame ={
    val spark = SparkSession.builder().appName("hiveApp").config(conf).enableHiveSupport().getOrCreate()

    var hiveRdd = spark.sql("select * from table_area")

    hiveRdd.write.mode(SaveMode.Append).saveAsTable("table_area_test")

    hiveRdd.show()

    hiveRdd
  }

  private def hiveData1(spark:SparkSession,conf:SparkConf): DataFrame ={
    println("hiveData1")
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:hive2://node3:10000/")
      .option("dbtable", "table_area_test")
      .option("diver","org.apache.hive.jdbc.HiveDriver")
      .option("user","root")
      .option("password","12345")
      .load()
    jdbcDF.show()
    jdbcDF
  }

}
