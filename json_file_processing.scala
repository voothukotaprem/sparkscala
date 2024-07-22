package spark_programs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object json_file_processing {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("json_file_process").master("local[*]").getOrCreate()
    val rawDF = spark.read.option("multiline",true).json("D:\\spark\\input\\a.json")
    rawDF.printSchema()

    println("==========sampleDF==============")
    val sampleDF = rawDF.withColumnRenamed("id", "key")
    sampleDF.printSchema()

    //for array -- we are doing explode function
    //for struct of array or struct -- we are using dot(.) along with column name eg: name.first_name

    //Extract batter element from the batters which is Struct of an Array and check the schema.

    val batDF = sampleDF.select("key", "batters.batter")
    batDF.printSchema()
    batDF.show(1,false)

    //create a separate row for each element of “batter” array by exploding “batter” column.
    import spark.implicits._

    //explode is spark function
    val bat2DF = batDF.select($"key", explode($"batter").alias("new_batter"))
    bat2DF.show()
    bat2DF.printSchema()

    //extract the individual elements from the “new_batter” struct.
    // We can use a dot (“.”) operator to extract the individual element or we can use “*” with dot (“.”) operator to select all the elements.

    bat2DF.select("key", "new_batter.id","new_batter.type").withColumnRenamed("id", "bat_id")
      .withColumnRenamed("type", "bat_type").show()


    val topDF = sampleDF.select($"key",explode($"topping").as("topping_new"))
    topDF.show(10,false)
    topDF.printSchema()



    val topDF2 =topDF.select("key","topping_new.*").withColumnRenamed("id","topping_id")
      .withColumnRenamed("type","topping_type").show(10,false)





  }

}
