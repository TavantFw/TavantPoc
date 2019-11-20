import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._

object dataStorage {
  def storeData(ds :Dataset[FinalCreditInfo],dataOutputpath:String,format:String)={
    import ds.sparkSession.implicits._
    format match{
      case "json"=>ds.write.mode("overwrite").json(dataOutputpath)
      case "csv"=>ds.write.mode("overwrite").partitionBy("Gender").csv(dataOutputpath)
      case _=>ds.write.mode("overwrite").save(dataOutputpath)
    }





  }
  //using DataFrame
  /*def storeDataAsDF(ds :DataFrame, dataOutputpath:String, format:String)={
    import ds.sparkSession.implicits._
    format match{
      case "json"=>ds.write.mode("overwrite").json(dataOutputpath)
      case "csv"=> ds.write.mode("overwrite").partitionBy("Gender").csv(dataOutputpath)
      case _=>ds.write.mode("overwrite").save(dataOutputpath)
    }

  }*/

}
