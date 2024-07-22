package spark_programs

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

object customer_purchase_analysis {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark SQL basic example").master("local[*]")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    //using createDataFrame

    val schema = StructType( Array(
      StructField("customer_id", IntegerType,true),
      StructField("purchase_amount", IntegerType,true) ,
      StructField("purchase_date", StringType,true)

    ))


    val data = spark.sparkContext.parallelize(
                                              Seq(Row(1,100,"2023-01-15"),
                                                Row(2,150,"2023-02-20"),
                                                Row(1,200,"2023-03-10"),
                                                Row(3,50,"2023-04-05"),
                                                Row(2,120,"2023-05-15"),
                                                Row(1,300,"2023-06-25")))

    val df= spark.createDataFrame(data,schema)



    df.printSchema()
    df.show()

    val total_purchase_by_customer = df.groupBy("customer_id").sum("purchase_amount").orderBy("customer_id")
    total_purchase_by_customer.printSchema()
    total_purchase_by_customer.show()
    //we can follow below too (by adding agg method)
    val total_purchase_by_customerc = df.groupBy("customer_id").agg(sum("purchase_amount").as("sum_amount")).orderBy(col("customer_id").desc)
    total_purchase_by_customerc.printSchema()
    total_purchase_by_customerc.show()

    val total_purchase_by_customerb = df.groupBy("customer_id").agg(sum("purchase_amount").as("sum_amount"),max("purchase_amount").as("highest_amount")).orderBy("customer_id")
    total_purchase_by_customerb.printSchema()
    total_purchase_by_customerb.show()
    spark.stop()

  }

}
