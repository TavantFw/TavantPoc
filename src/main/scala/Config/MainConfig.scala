package Config



<<<<<<< HEAD
case class MainConfig(datareaderconfig:  DataReaderConfig  ){

  datareaderconfig.isValid
=======
case class MainConfig(datareaderconfig:  DataReaderConfig ,datawriterconfig:DataWriterConfig ){

  datareaderconfig.isValid
  datawriterconfig.isValid
>>>>>>> origin/Reshma
}