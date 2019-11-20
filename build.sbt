name := "TavantPoc"

version := "0.1"

scalaVersion := "2.11.11"

val sparkVersion = "2.4.0"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion ,
  "org.scalatest" %% "scalatest" % "3.0.4" % "test",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
  "com.github.scala-incubator.io" %% "scala-io-file" % "0.4.3-1",
  "com.github.pureconfig" %% "pureconfig" % "0.12.1")
