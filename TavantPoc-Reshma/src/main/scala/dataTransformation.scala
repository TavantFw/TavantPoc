import CaseClass.FinalCreditInfo
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{lit, round, when}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import Main.spark

class dataTransformation {

  def transformData(ds: Dataset[FinalCreditInfo]): Dataset[FinalCreditInfo] = {
    import ds.sparkSession.implicits._
    val tranformedData = ds.withColumn("GenderData", when(ds("Gender") === "a", "Male")
      .otherwise(("Female")))
      .withColumn("ApprovedData", when(ds("Approved") === "+", "Approved")
        .otherwise("NotApproved")).filter(ds("Married") =!= "?")
      .withColumn("MarriedData", when(ds("Married") === "u", "Single").when(ds("Married") === "y", "Married")
        .otherwise("other"))
    tranformedData.select($"SlNo", round($"Age") as "Age", $"CreditScore", $"DebtinLakhs", $"Income" * 1000 as "Income", $"YearEmployed", $"GenderData" as "Gender", $"ApprovedData" as "Approved", $"MarriedData" as "Married").as[FinalCreditInfo]
  }

  def addIdentityColumn(data: DataFrame): DataFrame = {

    val schema = data.schema
    val rows = data.rdd.zipWithUniqueId.map {
      case (r: Row, id: Long) => Row.fromSeq(id +: r.toSeq)
    }

    val dfWithPK = spark.createDataFrame(
      rows, StructType(StructField("id", LongType, false) +: schema.fields))

    dfWithPK
  }


  def addingCorrectValues(data: DataFrame): DataFrame = {

    import spark.implicits._

    data.withColumn("Gender", when($"Gender" === "b", "M") when($"Gender" === "a", "F"))
      .withColumn("Approved", when($"Approved" === "+", "Yes") when($"Approved" === "-", "No"))
      .withColumn("Married", when($"Married" === "y", "Married") when($"Married" === "u", "Single")).withColumn("DebtinLakhs", $"DebtinLakhs" * 100)
  }
}
