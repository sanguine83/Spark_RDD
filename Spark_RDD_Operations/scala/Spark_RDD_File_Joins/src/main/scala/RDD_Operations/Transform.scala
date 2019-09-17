package RDD_Operations

import java.util.Calendar

import scala.io.Source._
import org.apache.spark._

class Transform_RDD(val sc: SparkContext) {

  // Read Configuration from file
  def parseProperties(filename: String): Map[String,String] = {
    val lines = fromFile(filename).getLines.toSeq
    val cleanLines = lines.map(_.trim).filter(!_.startsWith("#")).filter(_.contains("="))
    cleanLines.map(line => { val Array(a,b) = line.split("=",2); (a.trim, b.trim)}).toMap
  }

  val prop_map = parseProperties("/home/suresh/Spark_RDD_Operations/config/config.properties")

  val Customer_File = prop_map.getOrElse("Customer_File", "Not Found!")
  val Sales_File = prop_map.getOrElse("Sales_File","Not Found!")
  val Output_DIR = prop_map.getOrElse("Output_DIR","Not Found!")
  val Output_File = prop_map.getOrElse("Output_File","Not Found!")

  def transform_rdd(sc: SparkContext): Unit = {

    // Get Input RDD's
   // val customer_rdd = sc.textFile("file:///"+Customer_File)
   // val sales_rdd = sc.textFile("file:///"+Sales_File)
    val customer_rdd = sc.textFile(Customer_File)
    val sales_rdd = sc.textFile(Sales_File)

    // Remove Header
    val header_c = customer_rdd.first()
    val customer_rem_hdr = customer_rdd.filter(m => m != header_c)
    val header_s = sales_rdd.first()
    val sales_rem_hdr = sales_rdd.filter(m => m != header_s)

    // Split Input RDD and filter columns
    val customer_split = customer_rem_hdr.map(m => m.split("#")).map(array => (array(0), array(4)))
    val sales_split = sales_rem_hdr.map(m => m.split("#")).map(array => (array(1), array(0), array(2)))

    // Create Pair RDD's
    val pair_cus = customer_split.keyBy(t => t._1)
    val pair_sal = sales_split.keyBy(t => t._1)

    // Join Customer and Sales RDD's based on customer_id
    val join_rdd = pair_cus.join(pair_sal)

    // Combine values of 2 Files and Remove Key
    val combine_val = join_rdd.map(
      {
        case (customer_id,((customer_id_1,state),(customer_id_2,time,sales))) => (customer_id,(state,time,sales))
      }
    )
    val values = combine_val.values

    // Create Pair RDD with state, time as keys for aggregation on sales and finally remove key
    val final_key = values.map(
      {
        case (state, time, sales) => ((state,new EpochConvert(time.toLong).epochToDate(time.toLong)),(sales.toFloat))
      })
      .reduceByKey((x, y) => (x+y))
      .sortByKey()
      .map({case ((state,time),sales) => (state,time,sales)})

    // Collect RDD from the nodes and Save output file with de-limiter
    final_key.map{ x =>x.productIterator.mkString("#") }.saveAsTextFile(Output_DIR)

    // Rename the files
    //val save_Output = new SaveO
    // utput(Output_DIR, Output_File)
    println("End Of Spark Program: " + Calendar.getInstance().getTime())

  }
}

