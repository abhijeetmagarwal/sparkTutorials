package com.tutorials
import com.sun.xml.internal.bind.v2.TODO
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import org.apache.log4j.Level


object csvToDataframe extends App {

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

  /*val lines = spark.sparkContext.textFile("file:///C:/logs/a.txt")

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

  case class Person(name: String, age: Int)

  val personDS = Seq(Person("Max", 33), Person("Adam", 32), Person("Muller", 62)).toDS()
  personDS.show()

  val rddExample1 = spark.sparkContext.parallelize(Seq((1, "Spark"), (2, "Databricks")))
  val integerDS = rddExample1.toDS()
  integerDS.show()

  case class Company(name: String, foundingYear: Int, numEmployees: Int)
  val inputSeq = Seq(Company("ABC", 1998, 310), Company("XYZ", 1983, 904), Company("NOP", 2005, 83))
  val dfCompany = spark.sparkContext.parallelize(inputSeq).toDF()

  val companyDS = dfCompany.as[Company]
  companyDS.show()


  val rdd = spark.sparkContext.parallelize(Seq((1, "Spark"), (2, "Databricks"), (3, "Notebook")))
  val df = rdd.toDF("Id", "Name")

  val dataset = df.as[(Int, String)]
  dataset.show()


  val wordsDataset = spark.sparkContext.parallelize(Seq("Spark I am your father", "May the spark be with you", "Spark I am your father")).toDS()
  val groupedDataset = wordsDataset.flatMap(_.toLowerCase.split(" "))
    .filter(_ != "")
    .groupBy("value")
  val countsDataset = groupedDataset.count()
  countsDataset.show()


  case class Employee(name: String, age: Int, departmentId: Int, salary: Double)
  case class Department(id: Int, name: String)

  case class Record(name: String, age: Int, salary: Double, departmentId: Int, departmentName: String)
  case class ResultSet(departmentId: Int, departmentName: String, avgSalary: Double)

  val employeeDataSet1 = spark.sparkContext.parallelize(Seq(Employee("Max", 22, 1, 100000.0), Employee("Adam", 33, 2, 93000.0), Employee("Eve", 35, 2, 89999.0), Employee("Muller", 39, 3, 120000.0))).toDS()
  val employeeDataSet2 = spark.sparkContext.parallelize(Seq(Employee("John", 26, 1, 990000.0), Employee("Joe", 38, 3, 115000.0))).toDS()
  val departmentDataSet = spark.sparkContext.parallelize(Seq(Department(1, "Engineering"), Department(2, "Marketing"), Department(3, "Sales"))).toDS()

  val employeeDataset = employeeDataSet1.union(employeeDataSet2)

  def averageSalary(key: (Int, String), iterator: Iterator[Record]): ResultSet = {
    val (total, count) = iterator.foldLeft(0.0, 0.0) {
      case ((total, count), x) => (total + x.salary, count + 1)
    }
    ResultSet(key._1, key._2, total/count)
  }

  val averageSalaryDataset = employeeDataset.joinWith(departmentDataSet, $"departmentId" === $"id", "inner")
    .map(record => Record(record._1.name, record._1.age, record._1.salary, record._1.departmentId, record._2.name))
    .filter(record => record.age > 25)
    .groupBy($"departmentId", $"departmentName")
    .avg()

  averageSalaryDataset.show()




}


