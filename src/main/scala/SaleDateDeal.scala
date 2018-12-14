import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * ${DESCRIPTION}
  *
  * @author yansen
  * @create 2018-12-14 17:41
  **/
object SaleDateDeal {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.ERROR);
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);

    val spark = SparkSession.builder().enableHiveSupport().appName(s"${this.getClass.getSimpleName}").getOrCreate();

    spark.sql("")
    spark


  }
}
