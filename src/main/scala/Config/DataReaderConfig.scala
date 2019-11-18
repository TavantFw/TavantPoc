package Config

import scala.io._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.typesafe.scalalogging.StrictLogging

case class DataReaderConfig(columns: Option[List[String]], datacolumncheck: Option[String],
                            totalcolumn: Option[Int], datainputpath: Option[String])extends  usableConfigurable with StrictLogging{

  var fileTotalColumn = 0


  def isValid: Boolean = {

    //areValidColumns() && isValidDataColumnStart()

     if(isValidDataInputPath(datainputpath)){

       readJsonConfig()

     }

    areValidColumns()


  }

  def readJsonConfig():Unit={

        val json = Source.fromFile(datainputpath.get)
        val mapper = new ObjectMapper() with ScalaObjectMapper
        mapper.registerModule(DefaultScalaModule)
        val parsedJson = mapper.readValue[Map[String,Object]](json.reader())
        fileTotalColumn = parsedJson.keys.size

    }


  def areValidColumns(): Boolean = {
    def valid = fileTotalColumn.equals(totalcolumn.get)

    if (!valid) {
      logger.error("Number of columns not match with specified metadata file")
      sys.exit(1)
    }
    valid
  }

  def isValidDataColumnStart(): Boolean = {
    val valid = datacolumncheck.isDefined

    if (!valid) {
      logger.error(s"Invalid config: 'dataColumnStart' must be defined and it must be >= 0 and < size of column list")
    }
    valid
  }


}
