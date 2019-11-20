import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{lit, round, when}
import org.apache.spark.sql.functions._

object dataTransformation {
  // def removeNullCreditInfo(ds :Dataset[BussinessCreditInfo]):Dataset[BussinessCreditInfo]={

  def transformData(ds: Dataset[BussinessCreditInfo]): Dataset[FinalCreditInfo] = {
    import ds.sparkSession.implicits._
    val tranformedData = ds.withColumn("GenderData", when(ds("Gender") === "a", "Male").otherwise(lit("Female"))).withColumn("ApprovedData", when(ds("Approved") === "+", "Approved").otherwise(lit("NotApproved"))).filter(ds("Married") !== "?").withColumn("MarriedData", when(ds("Married") === "u", "Single").when(ds("Married") === "y", "Married").otherwise(lit("other"))).withColumn("SlNo", monotonically_increasing_id)
    tranformedData.select($"SlNo", round($"Age") as "Age", $"CreditScore", $"DebtinLakhs", $"Income" * 1000 as "Income", $"YearEmployed", $"GenderData" as "Gender", $"ApprovedData" as "Approved", $"MarriedData" as "Married").as[FinalCreditInfo]
  }
   //Using DataFrame
  /*def transformDataDF(ds: DataFrame): DataFrame = {
    import ds.sparkSession.implicits._
    val tranformedData = ds.withColumn("GenderData", when(ds("Gender") === "a", "Male").otherwise(lit("Female"))).withColumn("ApprovedData", when(ds("Approved") === "+", "Approved").otherwise(lit("NotApproved"))).filter(ds("Married") !== "?").withColumn("MarriedData", when(ds("Married") === "u", "Single").when(ds("Married") === "y", "Married").otherwise(lit("other"))).withColumn("SlNo", monotonically_increasing_id)
    tranformedData.select($"SlNo", round($"Age") as "Age", $"CreditScore", $"DebtinLakhs", $"Income" * 1000 as "Income", $"YearEmployed", $"GenderData" as "Gender", $"ApprovedData" as "Approved", $"MarriedData" as "Married")
  }*/
}
