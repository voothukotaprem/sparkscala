package spark_programs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object lag_lead {

  val spark = SparkSession.builder().master("local[*]").appName("lagandlead").getOrCreate()

  import spark.implicits._

  val data = Seq(("John", "2023-07-01", 5000),
    ("Jane", "2023-07-01", 7000),
    ("John", "2023-08-01", 6000),
    ("Jane", "2023-08-01", 8000),
    ("John", "2023-09-01", 5500),
    ("Jane", "2023-09-01", 7500)).toDF("sales_rep","date","sales_amount")

  data.printSchema()
  data.show(10,false)

  //# Define the window specification
  val window = Window.partitionBy("sales_rep").orderBy("date")

  //# Apply the PySpark Dense_RANK, LAG, and LEAD functions








}
