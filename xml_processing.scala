package spark_programs

import org.apache.spark.sql.SparkSession


object xml_processing {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("xml_processing").master("local[*]").getOrCreate()
    val inputDF = spark.read.format("com.databricks.spark.xml").option("rootTag","CATALOG").option("rowTag","PLANT")
                  .load("D:\\spark\\xml\\book.xml")

    inputDF.printSchema()
    inputDF.show(100,false)
  }

}
