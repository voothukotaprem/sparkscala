//package spark_programs
//
//import org.apache.spark.sql.SparkSession
////import org.apache.spark.sql.functions.broadcast
//
//
//case class Employee(name:String, age:Int, depId: String)
//case class Department(id: String, name: String)
//
//object broadcast {
//
//  def main(args: Array[String]): Unit = {
//
//    val spark = SparkSession.builder().master("local[*]").appName("broadcast_example").getOrCreate()
//    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 104857600)
//
//
//    import spark.implicits._   //with out this .toDF() wont work
//    import org.apache.spark.sql.functions.broadcast  //without this broadcast doent work and outside the class its not working
//
//    val employeeRDD = spark.sparkContext.parallelize(Seq(
//                                                    Employee("Mary", 33, "IT"),
//                                                    Employee("Paul", 45, "IT"),
//                                                    Employee("Peter", 26, "MKT"),
//                                                    Employee("Jon", 34, "MKT"),
//                                                    Employee("Sarah", 29, "IT"),
//                                                    Employee("Steve", 21, "Intern")))
//
//    val departmentRDD = spark.sparkContext.parallelize(Seq(
//                                                      Department("IT", "IT  Department"),
//                                                      Department("MKT", "Marketing Department"),
//                                                      Department("FIN", "Finance & Controlling")
//                                                    ))
//
//
//    val employeeDF = employeeRDD.toDF()
//    val departmentDF = departmentRDD.toDF()
//
//    // materializing the department data
////    val tmpDepartments = broadcast(departmentDF.as("departments"))
//
//    employeeDF.join(broadcast(departmentDF), $"depId" === $"id","inner").show()
//
//
//
//
//
//
//
//  }
//
//}
