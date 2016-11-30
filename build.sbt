spName := "mariusvniekerk/spark-jython-udf"

spAppendScalaVersion := true

organization := "mariusvniekerk"

name := "spark-jython-udf"

version := "0.0.1"

scalaVersion := "2.11.8"

sparkVersion := "2.0.1"

sparkComponents ++= Seq("sql")

libraryDependencies ++= Seq(
  "org.python" % "jython-standalone" % "2.7.0"
)

