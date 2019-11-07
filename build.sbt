name := "TavantPoc"

version := "0.1"

scalaVersion := "2.11.11"

val sparkVersion = "2.3.1"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion ,
  "org.scalatest" %% "scalatest" % "3.0.4" % "test")
