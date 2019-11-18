package Config
import com.typesafe.scalalogging.StrictLogging
import scalax.file.Path


trait usableConfigurable extends StrictLogging {



  def isValidDataInputPath(dataInputPath: Option[String]):Boolean = {

    logger.info("Validating the input file data ")

    dataInputPath match {
      case  Some(path) =>
        if (existsFilePath(path) && fileFormat(path)) {

          true

        } else {
          logger.error(s"The provided file path or format  (path:${dataInputPath.get}) does not exist.")
          false
        }
      case _  =>
        logger.error("Please provide the correct path to the data input.")
        false
    }
  }

  def existsFilePath(path:String):Boolean ={
    Path.fromString(path).exists && Path.fromString(path).isFile
  }
  def fileFormat(path:String):Boolean={
    path.split("\\.").last == "json"
  }

}