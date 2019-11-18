package Config



case class MainConfig(datareaderconfig:  DataReaderConfig  ){

  datareaderconfig.isValid
}