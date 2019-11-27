
import CaseClass.{CreditInfo, FinalCreditInfo}
import Main.spark
import org.apache.spark.sql.functions.{explode, from_json, lit}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

class dataPreparation {

  def dataPreparation(dataInputPath: String): Dataset[FinalCreditInfo] = {

    val json = spark.read.json(dataInputPath)
      .withColumnRenamed("Debt in Lakhs", "DebtinLakhs")
      .withColumnRenamed("Income in 000's", "Income")
      .withColumnRenamed("Education Level", "Education")
      .toJSON

        removeNullCreditInfo(jsonToFinalCreditInfo(json))
  }

  def jsonToFinalCreditInfo(json: Dataset[String])(implicit spark:
  SparkSession): Dataset[FinalCreditInfo] = {

    import spark.implicits._
    val jsnSchema = Seq.empty[CreditInfo].toDS().schema
    val schema = ArrayType(jsnSchema)
    val arrayColumn = from_json($"value", schema)
    val ds = json.select(explode(arrayColumn).alias("v"))
      .select("v.*")
      .as[CreditInfo]

    ds.withColumn("SlNo", monotonically_increasing_id)
      .select($"SlNo",
        $"Gender".cast(StringType),
        $"Age".cast(IntegerType),
        $"Married".cast(StringType),
        $"DebtinLakhs".cast(DoubleType),
        $"YearEmployed".cast(StringType),
        $"Education".cast(StringType),
        $"CreditScore".cast(StringType),
        $"Income".cast(DoubleType),
        $"Approved".cast(StringType))
      .as[FinalCreditInfo]

  }

  def removeNullCreditInfo(ds: Dataset[FinalCreditInfo]): Dataset[FinalCreditInfo] = {
    ds.filter(col("Income") =!= lit(0.0))
  }
}
