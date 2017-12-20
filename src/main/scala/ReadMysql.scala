import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * ${DESCRIPTION}
  *
  * @author yansen
  * @create 2017-11-13 12:07
  **/

//  hive-site.xml放到spark/conf目录下
//  1、启动hive
//  2、初始化hivecontext
//  3、打包运行

object ReadMysql {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setAppName("WorkCount").setMaster("local")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    var url = "jdbc:mysql://10.237.14.235:3306/xm_ci_zg?useUnicode=true&characterEncoding=utf-8"
    var table = "area"

    val reader = sqlContext.read.format("jdbc")
    reader.option("url",url)
    reader.option("dbtable",table)
    reader.option("diver","com.mysql.jdbc.Driver")
    reader.option("user","zg_root")
    reader.option("password","7658032")
    val df = reader.load();
    df.show()
  }

  private def runJDBCDataSource(spark:SparkSession): Unit ={
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://10.237.14.235:3306/xm_ci_zg?useUnicode=true&characterEncoding=utf-8")
      .option("dbtable", "sec_user") //必须写表名
      .option("diver","com.mysql.jdbc.Driver")
      .option("user","zg_root")
      .option("password","7658032")
      .load()
    //虚拟表
    jdbcDF.select("username", "name", "telephone").write.mode("overwrite").saveAsTable("user_abel")
    val jdbcSQl = spark.sql("select * from user_abel where name like '王%' ")
    println("mysql 表")
    jdbcSQl.show()
  }
}
