import java.io.DataInput

import Config._
import com.typesafe.scalalogging.StrictLogging
import dataPreparation.jsonToCreditInfo
import dataPreparation._
import dataTransformation._
import dataStorage._
import org.apache.spark.sql.{Dataset, SparkSession,DataFrame}
import org.apache.log4j.Logger
import dataPreparationDF._
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
     // preparing the data for consumption

     val prepareDataset=dataPreparation(dataInputPath) //datapreparation using the dataset

     val prepareDataframe=dataPreparationDF(dataInputPath)//datapreparation using the dataframe

     //
    val finalDataset=transformData(prepareDataset) //Transformation
    val finalDataframe = addingCorrectValues(spark, addIdentityColumn(spark, prepareDataframe))

     //reading output_path and format from Conf file
     val dataOutputPath = fileMetadata.datawriterconfig.dataoutputpath.get
     val format=fileMetadata.datawriterconfig.format.get

     //storing the data with dataset
     storeData(finalDataset,dataOutputPath+"\\DS",format)// Storage level

     //storing data with dataframe

     storeDataAsDF(finalDataframe,dataOutputPath+"\\DF",format)

   }


    def dataPreparation(dataInputPath: String):Dataset[BussinessCreditInfo]={

      val json = spark.read.json(dataInputPath)
        .withColumnRenamed("Debt in Lakhs","DebtinLakhs").
        withColumnRenamed("Income in 000's","Income")
        .withColumnRenamed("Education Level","Education").toJSON

      val  ds = removeNullCreditInfo(bussinessCreditInfo(jsonToCreditInfo(json)))
      ds
    }

    def dataPreparationDF(dataInputPath: String): DataFrame = {

      val dsjson =spark.read.json(dataInputPath)

        .withColumnRenamed("Debt in Lakhs","DebtinLakhs").

        withColumnRenamed("Income in 000's","Income")

        .withColumnRenamed("Education Level", "Education")


      //filtering the data and giving proper schema

      bussinessCreditInfoDF(removeNullCreditInfoDF((dsjson)))



    }
  }



}
