name := "kaggleAnalytics"

version := "0.1"

scalaVersion := "2.12.8"
val sparkVersion = "2.4.2"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)
