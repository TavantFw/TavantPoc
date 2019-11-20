import Config.configHandler
import Transformation._
import com.typesafe.scalalogging.StrictLogging
import dataPreparation._
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

object Main extends  StrictLogging{

  def main(args: Array[String]): Unit ={

    Logger.getRootLogger
    implicit val spark: SparkSession =
      SparkSession.builder.master("local[*]").getOrCreate()

     run()

   def run()={

     val fileMetadata = configHandler.getConfig()  //reading configuration and doing the validation
     logger.info(s" File metadata info   :${fileMetadata}")
     val dataInputPath = fileMetadata.datareaderconfig.datainputpath.get
     val FilteredDataFrame = dataPreparation(dataInputPath) // preparing the data for consumption

     //Doing Transformation
     val TranDataframe = addingCorrectValues(spark, addIdentityColumn(spark, FilteredDataFrame))


     println(TranDataframe.count())
     TranDataframe.show()
     TranDataframe.printSchema()

     // Storage level
     TranDataframe.write.format("json").save("TavantPoc\\resources\\")
   }


    def dataPreparation(dataInputPath: String): DataFrame = {

      val dsjson = spark.read.json(dataInputPath)
        .withColumnRenamed("Debt in Lakhs","DebtinLakhs").
        withColumnRenamed("Income in 000's","Income")
        .withColumnRenamed("Education Level", "Education")

      //filtering the data and giving proper schema
      bussinessCreditInfo(removeNullCreditInfo((dsjson)))

    }


    // val json = Source.fromFile(fileMetadata.datareaderconfig.datainputpath.get)












  }



}
