

import org.apache.spark.sql.functions.when
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Transformation {

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
