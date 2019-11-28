import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{lit, round, when}
import org.apache.spark.sql.functions._

import org.apache.spark.sql.functions.when

import org.apache.spark.sql.types.{LongType, StructField, StructType}

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object dataTransformation {
  // def removeNullCreditInfo(ds :Dataset[BussinessCreditInfo]):Dataset[BussinessCreditInfo]={

  def transformData(ds: Dataset[BussinessCreditInfo]): Dataset[FinalCreditInfo] = {
    import ds.sparkSession.implicits._
    val tranformedData = ds.withColumn("GenderData", when(ds("Gender") === "a", "Male")
      .otherwise(("Female")))
      .withColumn("ApprovedData", when(ds("Approved") === "+", "Approved")
      .otherwise("NotApproved")).filter(ds("Married") =!= "?")
      .withColumn("MarriedData", when(ds("Married") === "u", "Single").when(ds("Married") === "y", "Married")
      .otherwise("other"))
      .withColumn("SlNo", monotonically_increasing_id)
    tranformedData.select($"SlNo", round($"Age") as "Age", $"CreditScore", $"DebtinLakhs", $"Income" * 1000 as "Income", $"YearEmployed", $"GenderData" as "Gender", $"ApprovedData" as "Approved", $"MarriedData" as "Married").as[FinalCreditInfo]
  }
   //Using DataFrame
  /*def transformDataDF(ds: DataFrame): DataFrame = {
    import ds.sparkSession.implicits._
    val tranformedData = ds.withColumn("GenderData", when(ds("Gender") === "a", "Male").otherwise(lit("Female"))).withColumn("ApprovedData", when(ds("Approved") === "+", "Approved").otherwise(lit("NotApproved"))).filter(ds("Married") !== "?").withColumn("MarriedData", when(ds("Married") === "u", "Single").when(ds("Married") === "y", "Married").otherwise(lit("other"))).withColumn("SlNo", monotonically_increasing_id)
    tranformedData.select($"SlNo", round($"Age") as "Age", $"CreditScore", $"DebtinLakhs", $"Income" * 1000 as "Income", $"YearEmployed", $"GenderData" as "Gender", $"ApprovedData" as "Approved", $"MarriedData" as "Married")
  }*/
  def addIdentityColumn(spark: SparkSession, data: DataFrame): DataFrame = {

    val schema = data.schema

    val rows = data.rdd.zipWithUniqueId.map {

      case (r: Row, id: Long) => Row.fromSeq(id +: r.toSeq)

    }

    val dfWithPK = spark.createDataFrame(

      rows, StructType(StructField("id", LongType, false) +: schema.fields))

    return dfWithPK

  }



  def addingCorrectValues(spark: SparkSession, data: DataFrame): DataFrame = {

    import spark.implicits._

    data.withColumn("Gender", when($"Gender" === "b", "M") when($"Gender" === "a", "F"))

      .withColumn("Approved", when($"Approved" === "+", "Yes") when($"Approved" === "-", "No"))

      .withColumn("Married", when($"Married" === "y", "Married") when($"Married" === "u", "Single")).withColumn("DebtinLakhs", $"DebtinLakhs" * 100)
  }
}
