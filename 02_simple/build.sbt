name := "Simple Project"

version := "1.0"

scalaVersion := "2.11.8"


libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.1.0",
  "org.apache.spark" % "spark-sql_2.11"  % "2.1.0",
  "au.com.bytecode" % "opencsv"  % "2.4"
)


