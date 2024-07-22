package spark_programs
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._



object json_Object_Processing {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("processing Json data").master("local[*]").getOrCreate()

    val data = """  [
      ['EMP001', '{"dept" : "account", "fname": "Ramesh", "lname": "Singh", "skills": ["excel", "tally", "word"]}'],
    ['EMP002', '{"dept" : "sales", "fname": "Siv", "lname": "Kumar", "skills": ["biking", "sales"]}'],
    ['EMP003', '{"dept" : "hr", "fname": "MS Raghvan", "skills": ["communication", "soft-skills"]}']
    ]"""
//val data = """  [
//      ['EMP001', '{"dept" : "account", "fname": "Ramesh", "lname": "Singh"}'],
//    ['EMP002', '{"dept" : "sales", "fname": "Siv", "lname": "Kumar"}'],
//    ['EMP003', '{"dept" : "hr", "fname": "MS Raghvan"}']
//    ]"""

    //    val json = """[
    //{"ID":1,"ATTR1":"ABC"},
    //{"ID":2,"ATTR1":"DEF"},
    //{"ID":3,"ATTR1":"GHI"}]"""

    val schema = StructType(Array(
      StructField("emp_num", StringType,false),
      StructField("raw_data", MapType(StringType, StringType,false))))

    //this is necessary
    import spark.implicits._

    val raw_dataset = Seq(data).toDS()

    val input_df = spark.read.json(raw_dataset).toDF("s","v")

//    val raw_data_df = spark.createDataFrame(data,schema)
    input_df.show(10,false)





  }


}
