package spark_programs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object json_studentfile_processing {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("json_studentfile_process").master("local[*]").getOrCreate()
    val rawDF = spark.read.option("multiline", true).json("D:\\spark\\json\\student.json")
    rawDF.printSchema()

    //for array -- we are doing explode function
    //for struct -- we are using dot(.) along with column name eg: name:first_name

    //for two arrays,we cant include two explodes in one select statement.For that ,we are going with "withcolumn" for each colum

  import spark.implicits._

      //for single array data

       val inputSingleArrayDF = rawDF.select(explode($"Employees").alias("employees_info"))
        inputSingleArrayDF.printSchema()
        inputSingleArrayDF.show(10,false)

        val employees_info = inputSingleArrayDF.select("employees_info.*")
        employees_info.printSchema()
        employees_info.show(20,false)

        //for double arrays column --- explode the column by adding a new column
        val inputDoubleArrayDF = rawDF.select("Employees","users")
        val explodeDF = inputDoubleArrayDF.withColumn("employees_info",explode($"Employees")).withColumn("users_info",explode($"users"))

        explodeDF.printSchema()
        explodeDF.show(10,false)

        //dropping actual columns after exploding columns
       val finalDF = explodeDF.drop("Employees","users")
        finalDF.printSchema()
        finalDF.show(10,false)

    //filer on lower case of first name other wise it will come as 25 columns (5*5 combinationss)
//        val employees_and_users_info = finalDF.select("employees_info.*","users_info.*")
        val employees_and_users_info = finalDF.select("employees_info.*","users_info.*").filter(lower($"employees_info.firstName") === lower($"users_info.firstName"))
        employees_and_users_info.printSchema()
        employees_and_users_info.show(100,false)


  }
}