package spark_programs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Online_Store_Orders {
  def main(args: Array[String]): Unit = {
    val spark =SparkSession.builder().master("local[*]").appName("online_store_orders").getOrCreate()
    val input =spark.read.option("header","true").csv("D:\\spark\\inputCSV\\store_orders.csv")
    input.printSchema()
    input.show(10,false)

//    💡𝐂𝐚𝐥𝐜𝐮𝐥𝐚𝐭𝐞 𝐭𝐡𝐞 𝐭𝐨𝐭𝐚𝐥 𝐫𝐞𝐯𝐞𝐧𝐮𝐞 𝐠𝐞𝐧𝐞𝐫𝐚𝐭𝐞𝐝 𝐟𝐫𝐨𝐦 𝐚𝐥𝐥 𝐨𝐫𝐝𝐞𝐫𝐬.
    val total_revenue = input.agg(sum("total_amount").alias("total revenue"))
    total_revenue.printSchema()
    total_revenue.show()

//    💡𝐅𝐢𝐧𝐝 𝐭𝐡𝐞 𝐚𝐯𝐞𝐫𝐚𝐠𝐞 𝐨𝐫𝐝𝐞𝐫 𝐚𝐦𝐨𝐮𝐧𝐭.

    val avg_amount = input.agg(avg("total_amount").alias("avg amount"))
    avg_amount.printSchema()
    avg_amount.show()

//    💡𝐈𝐝𝐞𝐧𝐭𝐢𝐟𝐲 𝐭𝐡𝐞 𝐡𝐢𝐠𝐡𝐞𝐬𝐭 𝐭𝐨𝐭𝐚𝐥 𝐨𝐫𝐝𝐞𝐫 𝐚𝐦𝐨𝐮𝐧𝐭 𝐚𝐧𝐝 𝐢𝐭𝐬 𝐜𝐨𝐫𝐫𝐞𝐬𝐩𝐨𝐧𝐝𝐢𝐧𝐠 𝐜𝐮𝐬𝐭𝐨𝐦𝐞𝐫.

    val highest_order_Amount1 = input.orderBy(desc("total_amount")).select(first("customer_id"),first("total_amount"))
    //or will use first or limit
    val highest_order_Amount = input.orderBy(desc("total_amount")).select("customer_id","total_amount").limit(1)
    highest_order_Amount.printSchema()
    highest_order_Amount.show()

//    💡𝐂𝐚𝐥𝐜𝐮𝐥𝐚𝐭𝐞 𝐭𝐡𝐞 𝐭𝐨𝐭𝐚𝐥 𝐧𝐮𝐦𝐛𝐞𝐫 𝐨𝐟 𝐨𝐫𝐝𝐞𝐫𝐬 𝐟𝐨𝐫 𝐞𝐚𝐜𝐡 𝐜𝐮𝐬𝐭𝐨𝐦𝐞𝐫.
    val number_of_orders = input.groupBy("customer_id").agg(count("order_id").alias("number_of_orders")).sort(desc("number_of_orders"))
    number_of_orders.show()

  }

}
