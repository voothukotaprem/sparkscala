package spark_programs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object employee_Analysis {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").appName("employee analysis").getOrCreate()

    val input_old =spark.read.option("header","true").csv("D:\\spark\\inputCSV\\employee.csv")

    //changing datatype from string to integer
    val input = input_old.withColumn("salary",col("salary").cast("int"))
    input.printSchema()
    input.show(10,false)


    //    Calculate the total payroll cost for the company.
    val total_payroll = input.agg(sum("salary").alias("total payroll"))
    total_payroll.printSchema()
    total_payroll.show()


//    💡Find the average salary for each department.
    val avg_salary_department = input.groupBy("department").agg(avg("salary").alias("avg_salary"))
    avg_salary_department.printSchema()
    avg_salary_department.show()

//    💡Identify the highest-paid employee and their department.
//
    val highest_paid_emp = input.orderBy(desc("salary")).select("employee_name","department","salary").limit(1)
    highest_paid_emp.printSchema()
    highest_paid_emp.show(10,false)

//    💡Calculate the total number of employees in each department.
    val total_emp = input.groupBy("department").count()
    //or
    val total_emp2 = input.groupBy("department").agg(count("employee_id").alias("count of employees"))
    total_emp2.printSchema()
    total_emp2.show()


    spark.stop()
  }

}
