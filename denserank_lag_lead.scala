package spark_programs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object denserank_lag_lead {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").appName("lagandlead").getOrCreate()

    import spark.implicits._

    val data = Seq(("John", "2023-07-01", 5000),
      ("Jane", "2023-07-01", 7000),
      ("John", "2023-08-01", 6000),
      ("Jane", "2023-08-01", 8000),
      ("John", "2023-09-01", 5500),
      ("Jane", "2023-09-01", 7500)).toDF("sales_rep", "date", "sales_amount")

    data.printSchema()
    data.show(10, false)

    //# Define the window specification
      val window = Window.partitionBy("sales_rep").orderBy("date")

    //# Apply the Spark Dense_RANK, LAG, and LEAD functions
    val df_ranked = data.withColumn("quarterly_rank", dense_rank().over(window))
    var df_analyzed = df_ranked.withColumn("previous_month_sales", lag("sales_amount",1).over(window))
     df_analyzed = df_analyzed.withColumn("next_month_sales", lead("sales_amount",1).over(window))
    df_analyzed.show(10,false)

  }

}
