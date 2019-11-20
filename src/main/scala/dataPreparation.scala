
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object dataPreparation {


  /*def jsonToCreditInfo(json:Dataset[String])(implicit spark:
  SparkSession):Dataset[CreditInfo]={

    import spark.implicits._
    //val ds:Dataset[String] =
    val jsnSchema = Seq.empty[CreditInfo].toDS().schema
    val schema = ArrayType(jsnSchema)
    val arrayColumn = from_json($"value",schema)

    json.select(explode(arrayColumn).alias("v"))
      .select("v.*")
     .as[CreditInfo]
  }*/

  def bussinessCreditInfo(ds: DataFrame): DataFrame = {
    import ds.sparkSession.implicits._
    ds.select($"Gender".cast(StringType),
      $"Age".cast(IntegerType),
      $"Married".cast(StringType),
      $"DebtinLakhs".cast(DoubleType),
      $"YearEmployed".cast(StringType),
      $"Education".cast(StringType),
      $"CreditScore".cast(StringType),
      $"Citizen".cast(StringType),
      $"ZipCode".cast(IntegerType),
      $"Income".cast(DoubleType),
      $"Approved".cast(StringType))


  }

  def removeNullCreditInfo(ds: DataFrame): DataFrame = {
    val notFollowingList = List("?", "l")
    val zipcodeBadElement = List("?", "0")
    //.filter(col("Income" ) =!=  lit(0.0))
    ds.filter(col("Gender") =!= "?").filter(not(ds("Married") isin (notFollowingList: _*))).filter(col("Age") =!= "?").filter(not(ds("ZipCode") isin (zipcodeBadElement: _*)))
    // .filter(col("ZipCode") =!= notFollowingList)
    //  ds.filter((col("Gender").isNotNull))


  }




}
