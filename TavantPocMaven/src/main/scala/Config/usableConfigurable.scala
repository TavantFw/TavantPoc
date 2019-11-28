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
  def isValidDataOutputPath(dataOutputPath: Option[String]):Boolean = {

    logger.info("Validating the output file path ")

    dataOutputPath match {
      case  Some(path) =>
        if (existsFilePath(path)) {

          true

        } else {
          logger.error(s"The provided file path  (path:${dataOutputPath.get}) does not exist.")
          false
        }
      case _  =>
        logger.error("Please provide the correct path to the data input.")
        false
    }
  }


  def existsFilePath(path:String):Boolean ={
    Path.fromString(path).exists
  }
  def fileFormat(path:String):Boolean={
    path.split("\\.").last == "json" && Path.fromString(path).isFile
  }

}