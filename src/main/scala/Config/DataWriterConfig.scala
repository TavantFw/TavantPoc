package Config
import com.typesafe.scalalogging.StrictLogging

case class DataWriterConfig(dataoutputpath:Option[String],format:Option[String])extends  usableConfigurable with StrictLogging {
  def isValid: Boolean = {
    if(isValidDataOutputPath(dataoutputpath)){

     true

    }else
    {
      false
    }
  }


}
