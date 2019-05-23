package src.main.scala.com.tutorials
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import org.apache.log4j.Level


class wordCount {

  println("Abhijeet")

}

object  wordCount extends  App
{

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  println("Abhijeet")
  System.setProperty("hadoop.home.dir", "C:\\winutils")


  val spark = SparkSession.builder()
    .appName("wordCount")
    .config("spark.broadcast.blockSize", 1024)
    .config("spark.master", "local")
    .getOrCreate()

  val data = List("b","c","d","e")

  //data.foreach(x => println(x))

  //val rdd = spark.sparkContext.parallelize(data)

  val lines = spark.sparkContext.textFile("file:///C:/logs/a.txt")

  lines.collect().foreach(println)

  lines.flatMap(x => x.split(",")).map(x => (x,1)).reduceByKey(_+_)
    .collect().foreach(println)



}
