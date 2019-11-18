import java.io.DataInput

import Config.{DataReaderConfig, configHandler}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.typesafe.scalalogging.StrictLogging
import dataPreparation.jsonToCreditInfo
import dataPreparation._
import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, SparkSession}
import org.mortbay.util.ajax.JSON

import scala.io.Source

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
     dataPreparation(dataInputPath)// preparing the data for consumption
    //Transformation
     // Storage level
   }


    def dataPreparation(dataInputPath: String):Unit={

      val json = spark.read.json(dataInputPath)
        .withColumnRenamed("Debt in Lakhs","DebtinLakhs").
        withColumnRenamed("Income in 000's","Income")
        .withColumnRenamed("Education Level","Education").toJSON

      val  ds = removeNullCreditInfo(bussinessCreditInfo(jsonToCreditInfo(json)))
      ds.show()
      ds.printSchema()

    }
   // val json = Source.fromFile(fileMetadata.datareaderconfig.datainputpath.get)












  }



}
