package RDD_Operations

import java.util.Calendar
import org.apache.spark._
import org.apache.log4j.Level
import org.apache.log4j.Logger

object Driver {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("Start Of Spark Program: " + Calendar.getInstance().getTime())

    // Get SPARK Context
    val conf = new SparkConf().setAppName("Spark_RDD")
    val sc = new SparkContext(conf)

    // Instantiate Transform RDD class
    val trans = new Transform_RDD(sc)
    trans.transform_rdd(sc)

  }
}

