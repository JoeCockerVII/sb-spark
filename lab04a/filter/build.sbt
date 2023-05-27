ThisBuild / version := "0.1"

ThisBuild / scalaVersion := "2.11.12"

val sparkVersion = "2.4.7"

lazy val root = (project in file("."))
  .settings {
    name := "lab04"
  }

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % sparkVersion,
  "org.apache.spark" % "spark-sql_2.11" % sparkVersion,
  "org.apache.spark" % "spark-catalyst_2.11" % sparkVersion
)
