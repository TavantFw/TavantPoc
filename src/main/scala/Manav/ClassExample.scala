package Manav

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

// Creating a Case class
case class Salary(deptName: String, empNo: Int, empName: String, salary: Long, skill: Seq[String])

object WindowFunctions{


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local")
      .appName("Window Functions")
      .getOrCreate()




    import spark.implicits._
    //Creating a Dataset
    val empSalary = Seq(
      Salary("Admin", 21, "Katam", 50000, List("Spark", "Scala")),
      Salary("HR", 22, "Karanpreet", 39000, List("Hiring", "Scala")),
      Salary("Admin", 33, "Rakesh", 48000, List("Spark", "Scala")),
      Salary("Admin", 44, "Ravi", 48000, List("Spark", "Scala")),
      Salary("HR", 55, "Suhani", 35000, List("Hiring", "Scala")),
      Salary("IT", 71, "Deepak", 42000, List("Spark", "Scala")),
      Salary("IT", 81, "Veeru", 60000, List("Spark", "Python")),
      Salary("IT", 90, "Manav", 45000, List("Spark", "Java")),
      Salary("IT", 101, "Pritam", 52000, List("Spark", "Scala")),
      Salary("IT", 112, "Abhishek", 52000, List("Spark", "Python"))).toDS()

    // empSalary.createTempView("empSalary")

    empSalary.show()

  }
}
