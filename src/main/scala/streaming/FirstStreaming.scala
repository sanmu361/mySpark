package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * ${DESCRIPTION}
  *
  * @author yansen
  * @create 2018-12-24 17:35
  **/
object FirstStreaming {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("streamingTest")

    val ssc = new StreamingContext(conf,Seconds(1))

    val lines= ssc.socketTextStream("localhost",9999)

    val words = lines.flatMap(_.split(" "))
    val paris = words.map(word => (word,1))
    val wordCounts = paris.reduceByKey(_ + _)

    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
