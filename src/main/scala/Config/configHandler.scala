package Config

import com.typesafe.scalalogging.StrictLogging
import pureconfig.ConfigSource
import Config.MainConfig
import scalax.file.Path
import scala.io._
import pureconfig.generic.auto._

object configHandler extends  StrictLogging {

  val projectPath = System.getProperty("user.dir")
  val propertiesFilePath = s"$projectPath/src/main/resources"
  val configName = "properties.conf"
  val userConfigPath = s"$propertiesFilePath/$configName"
 // val config: String= getConfig()


  def getConfig(): MainConfig = {

    if (!Path.fromString(userConfigPath).exists && Path.fromString(userConfigPath).isFile) {
      logger.error(s"The config file '$userConfigPath' does not exist.")
      sys.exit(1)
    }
    val config = ConfigSource.file(s"$userConfigPath").load[MainConfig]

    /*else {
      config match {

        case Right(i) => i.datareaderconfig.datainputpath match {
          case Some(j) =>
            for (line <- Source.fromFile(j)) {
              print(line)
            }
        }
      }

      }
*/

     //config.right.map(_.datareaderconfig.datainputpath.get)
    config match {

      case Left(conf) => logger.error(s" The config file properties is not m corrected ${conf}")
        sys.exit(1)
      case Right(conf) => conf


    }



    }



}