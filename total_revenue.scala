package spark_programs

import org.apache.spark.sql. SparkSession
import org.apache.spark.sql.functions._



object total_revenue {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("total_revenue").master("local[*]")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import spark.implicits._

    //toDF method
    val data = Seq((1,"Product A","Electronics",500,100),
        (2,"Product B","Clothing",50,200),
        (3,"Product C","Electronics",800,50),
        (4,"Product D","Beauty",30,300),
        (5,"Product E","Clothing",75,150))

    val df = data.toDF("product_id","product_name", "category","price","quantity_sold")

    df.printSchema()
    df.show()

    //1.Calculate the total revenue generated from all sales.

    val total_revenue_df = df.withColumn("total_revenue", df("price") * df("quantity_sold")).agg(sum("total_revenue").alias("total_Revenue_generated"))
    total_revenue_df.printSchema()
    total_revenue_df.show()

    //2.Find the top 3 best-selling category based on the quantity sold.

    val top_three_df = df.groupBy("category").agg(sum("quantity_sold").alias("sold")).orderBy(desc("sold")).limit(5)
    top_three_df.printSchema()
    top_three_df.show()

    //3.Find the top 5 best-selling products based on the quantity sold.

    val top_df = df.orderBy(desc("quantity_sold")).limit(5)
    top_df.printSchema()
    top_df.show(10,false)

    //4.Calculate the average price of products in each category.

    val avg_df = df.groupBy("category").agg(avg("price").alias("avg_price"))
    avg_df.printSchema()
    avg_df.show(10,false)

    //5.Identify the category with the highest total revenue.
    val highese_total_revenue_df = df.withColumn("total_revenue", df("price") * df("quantity_sold")).groupBy("category").agg(sum("total_revenue").alias("total_Revenue_generated")).orderBy(desc("total_Revenue_generated")).limit(1)
    highese_total_revenue_df.printSchema()
    highese_total_revenue_df.show()



    spark.stop()



  }

}
