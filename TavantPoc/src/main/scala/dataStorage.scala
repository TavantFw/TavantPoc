import CaseClass.FinalCreditInfo
import org.apache.spark.sql.{DataFrame, Dataset}

class dataStorage {

  def storeDataAsDS(ds :Dataset[FinalCreditInfo],dataOutputpath:String,format:String)={
    format match{
      case "json"=>ds.write.mode("overwrite").option("header",true).json(dataOutputpath)
      case "csv"=>ds.write.mode("overwrite").option("header",true).partitionBy("Gender").csv(dataOutputpath)
      case _=>ds.write.mode("overwrite").option("header",true).save(dataOutputpath)
    }
  }
  //using DataFrame
  def storeDataAsDF(ds :DataFrame, dataOutputpath:String, format:String)={
    format match{
      case "json"=>ds.write.mode("overwrite").option("header",true).json(dataOutputpath)
      case "csv"=> ds.write.mode("overwrite").option("header",true).partitionBy("Gender").csv(dataOutputpath)
      case _=>ds.write.mode("overwrite").option("header",true).save(dataOutputpath)
    }
  }

}
