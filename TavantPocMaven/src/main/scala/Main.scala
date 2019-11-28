import Config._
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger

object Main extends App with StrictLogging {

  Logger.getRootLogger
  implicit val spark: SparkSession = SparkSession.builder.master("local[*]").getOrCreate()

//    Reading Configuration & Validation Stage
    val fileMetadata = configHandler.getConfig()
    logger.info(s" File metadata info   :$fileMetadata")
    val dataInputPath = fileMetadata.datareaderconfig.datainputpath.get
    val dataOutputPath = fileMetadata.datawriterconfig.dataoutputpath.get
    val format = fileMetadata.datawriterconfig.format.get

//    Data Preparation Stage
    val dataprepards = new dataPreparation
    val prepareDataset = dataprepards.dataPreparation(dataInputPath) //DataSet
    val dataprepardf = new dataPreparationDF
    val prepareDataframe = dataprepardf.dataPreparationDF(dataInputPath) //DataFrame

//    Data Transformation Stage
    val datatransform = new dataTransformation
    val finalDataset = datatransform.transformData(prepareDataset) //DataSet
    val finalDataframe = datatransform.addingCorrectValues(datatransform.addIdentityColumn(prepareDataframe)) //DataFrame

//    Storage Stage
    val datastore = new dataStorage
    datastore.storeDataAsDS(finalDataset, dataOutputPath + "\\DS", format) //DataSet
    datastore.storeDataAsDF(finalDataframe, dataOutputPath + "\\DF", format) //DataFrame


}
