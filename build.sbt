name := "kathmandu-apache-spark-meetup-introduction-to-spark-2.0"

version := "1.0"

scalaVersion := "2.11.8"

lazy val sparkVersion = "2.0.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
//  "postgresql" % "postgresql" % "9.1-901-1.jdbc4",
  "com.typesafe.slick" %% "slick" % "3.1.0",
  "org.postgresql" % "postgresql" % "9.3-1100-jdbc41"
)