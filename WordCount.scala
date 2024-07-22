import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object WordCount {

  def main(args: Array[String]): Unit = {
    //D:\spark\spark-2.4.5-bin-hadoop2.7\bin>spark-shell
    //spark-shell
    val conf = new SparkConf().setAppName("Word Count").setMaster("local")
    val sc = new SparkContext(conf)
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\bin\\winutils")
    val input = sc.textFile("C:\\Users\\premv\\IdeaProjects\\Word Count\\project\\words")
    val count = input.flatMap(lines => lines.split(" "))
      .map(word=>(word,1))
      .reduceByKey(_+_)
      //.reduceByKey((x,y)=> (x+y))

    count.saveAsTextFile("C:\\Users\\premv\\IdeaProjects\\Word Count\\project\\output5")

val spark = SparkSession.builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val input2  = spark.read.text("C:\\Users\\premv\\IdeaProjects\\Word Count\\project\\words")

  //  val output2 =input2.write.
System.out.println("spark scala works fine")

    }

}
