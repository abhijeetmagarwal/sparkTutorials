package com.tutorials

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import org.apache.log4j.Level
import util.transformationRules.addUDF
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._

object dataframeExample extends App {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  println("Abhijeet")
  System.setProperty("hadoop.home.dir", "C:\\winutils")


  val spark = SparkSession.builder()
    .appName("wordCount")
    .config("spark.broadcast.blockSize", 1024)
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

/*
  val lines = spark.sparkContext.textFile("file:///C:/logs/a.txt")

  val df = spark
    .read
    .option("header",true)
    .csv("file:///D:/BigData/Machine Learning/Greyatom Barclays Data Science Training/Python/Pandas/pokemon - Copy.csv")

  /*val df =  spark.read
   .option("header", "true").option("inferSchema", "true")
   .load("file:///D:/BigData/Machine Learning/Greyatom Barclays Data Science Training/Python/Pandas/pokemon - Copy.csv")
*/
  df.printSchema()

  df.show(100,false)
*/

  // --------!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!--------------------
  case class Person(name: String, age: Option[Int])

  case class abhi(name:String,age:Int,valid:Boolean)

  val personDS = Seq(Person("Max", None ),Person("Max", Some(33)), Person("Adam", Some(32)),
    Person("Muller11", Some(62))).toDS()
  personDS.show()

  //personDS.map(x => abhi(x.name,x.age,True))

println("-------- Iterating over each element Start  ----------")
  val correcectdDF = personDS.flatMap { data =>

    val cleanName = checkString(data.name)

    (cleanName, Some(data.age)) match {
      case (name, Some(age)) =>
        val t = abhi(name = name, age =  age.getOrElse(0), valid = true)
        Some(t)
      case _ => None
    }
  }


  correcectdDF.collect().foreach(println)

  def checkString(element : String):String =
  {
    element.replaceAll("[0-9]", "")

  }

  println("-------- Iterating over each element End  ----------")



  personDS.createTempView("abc")

  spark.sql("""select * from abc""").show(1000,false)

  //------ UDF Tresting -------- Start ---------//

  val rddExample1 = spark.sparkContext.parallelize(Seq((1, "Spark"), (2, "Databricks")))
  val integerDS = rddExample1.toDS().withColumn("defaultValueToBeAdded",lit(10))
  integerDS.show()
  integerDS.printSchema()

  val integerDSNew = integerDS
    .withColumn("newColumn", addUDF(col("_1"),col("defaultValueToBeAdded")))

  integerDSNew.show(1000,false)

  //------ UDF Tresting -------- End ---------//

  case class Company(name: String, foundingYear: Int, numEmployees: Int)
  val inputSeq = Seq(Company("ABC", 1998, 310), Company("XYZ", 1983, 904), Company("NOP", 2005, 83))
  val dfCompany = spark.sparkContext.parallelize(inputSeq).toDF()






  val companyDS = dfCompany.as[Company]
  companyDS.show()


  val rdd = spark.sparkContext.parallelize(Seq((1, "Spark"), (2, "Databricks"), (3, "Notebook")))
  val df = rdd.toDF("Id", "Name")

  val dataset = df.as[(Int, String)]
  dataset.show()



}


