package spark_programs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lead,lag,_}
import org.apache.spark.sql.expressions.Window


object lag_windowPartitioning {

  def main(args: Array[String]): Unit = {

    val spark =SparkSession.builder().master("local[*]").appName("Lag_WindowPartitioning").getOrCreate()

//    🏹 CHALLENGE :
//      I have a data set which contains fields such as: item, event, timestamp, userid while lacking of the sessionId.
//    I'm expected to create a session _id which expires for every 30 minutes window.
//      This session _id should be unique per session per user.
//
//    Approach :
//      ✅ Create a lag function to get a previous value at window partitioning by userid and order by timestamp
//    ✅Defining the logic to create session_id which expires for every 30 mins using CASE WHEN statement.
//    ✅Calculating the running sum for window partitioning by user id and order by timestamp to create unique id per session.
//      ✅Applying dense_rank() window function to create unique session id per session per user.

    import spark.implicits._

    val input_data = Seq(("blue","view", "1610494094750",11),
      ("green","addtobag","1510593114350",21),
      ("red","close","1610493115350",41),
      ("blue","view","1610494094350",11),
      ("blue","close","1510593114312",21),
      ("red","view","1610493114350",41),
      ("red","view","1610593114350",41),
      ("green","purchase","1610494094350",31)).toDF("item","event","timestamp","userId")

    input_data.printSchema()
    input_data.show(10,false)

//    #Gettheprevioustimestampforeachuserid
 val window = Window.partitionBy("userid").orderBy("timestamp")
    val df1 =input_data.withColumn("session_id", lag("timestamp",1).over(window))
    df1.printSchema()
    df1.show()

//#Define if the session is the1 stone(more than1800 s after the previous one)

    val df2=df1.withColumn("session_id_new", when(col("timestamp") - col("session_id") <=1800,0).otherwise(1))
    df2.show()


   // #create a unique session id per session(same id can exists for different users)

    val df3=df2.withColumn("unique_session_id", sum("session_id_new").over(Window.partitionBy("userid").orderBy("timestamp")))
    df3.printSchema()
    df3.show(10,false)

//    #create a unique session id per session per user
    val df4=df3.withColumn("unique_session_id_per_user", dense_rank().over(Window.orderBy("userid","unique_session_id")))
    df4.show(10,false)






  }

}
