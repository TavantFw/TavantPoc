import java.io.DataInput

import Config._
import com.typesafe.scalalogging.StrictLogging
import dataPreparation.jsonToCreditInfo
import dataPreparation._
import dataTransformation._
import dataStorage._
import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, SparkSession}

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
     val dataOutputPath = fileMetadata.datawriterconfig.dataoutputpath.get
     val format=fileMetadata.datawriterconfig.format.get
     val prepareddata=dataPreparation(dataInputPath)// preparing the data for consumption
     val finalData=transformData(prepareddata) //Transformation
     storeData(finalData,dataOutputPath,format)// Storage level

   }


    def dataPreparation(dataInputPath: String):Dataset[BussinessCreditInfo]={

      val json = spark.read.json(dataInputPath)
        .withColumnRenamed("Debt in Lakhs","DebtinLakhs").
        withColumnRenamed("Income in 000's","Income")
        .withColumnRenamed("Education Level","Education").toJSON

      val  ds = removeNullCreditInfo(bussinessCreditInfo(jsonToCreditInfo(json)))
      ds
    }













  }



}
