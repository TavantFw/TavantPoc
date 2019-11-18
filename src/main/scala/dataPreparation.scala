
import org.apache.spark.sql.functions.{explode, from_json, lit}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

import org.apache.spark.sql.functions._

object dataPreparation {




  def jsonToCreditInfo(json:Dataset[String])(implicit spark:
  SparkSession):Dataset[CreditInfo]={

    import spark.implicits._
    //val ds:Dataset[String] =
    val jsnSchema = Seq.empty[CreditInfo].toDS().schema
    val schema = ArrayType(jsnSchema)
    val arrayColumn = from_json($"value",schema)

    json.select(explode(arrayColumn).alias("v"))
      .select("v.*")
     .as[CreditInfo]

  }

  def bussinessCreditInfo(ds :Dataset[CreditInfo]):Dataset[BussinessCreditInfo]={
    import ds.sparkSession.implicits._
    ds.select($"Gender".cast(StringType),
      $"Age".cast(IntegerType),
      $"DebtinLakhs".cast(DoubleType),
      $"YearEmployed".cast(StringType),
      $"Education".cast(StringType),
      $"CreditScore".cast(StringType),
      $"Income".cast(DoubleType),
      $"Approved".cast(StringType))
      .as[BussinessCreditInfo]

  }

  def removeNullCreditInfo(ds :Dataset[BussinessCreditInfo]):Dataset[BussinessCreditInfo]={


   ds.filter(col("Income" ) =!=  lit(0.0))


  }




}
