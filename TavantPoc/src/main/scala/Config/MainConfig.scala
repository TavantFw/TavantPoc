package Config



case class MainConfig(datareaderconfig:  DataReaderConfig ,datawriterconfig:DataWriterConfig ){

  datareaderconfig.isValid
  datawriterconfig.isValid
}