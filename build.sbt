ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"
autoScalaLibrary:=false
lazy val root = (project in file("."))
  .settings(
    name := "SparkPocProject"
  )

val sparkVersion ="3.2.3"
libraryDependencies +="org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies+= "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" % "spark-sql-kafka-0-10_2.12" % "3.2.0"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "3.2.0"
libraryDependencies += "com.github.jnr" % "jnr-posix" % "3.0.50"
libraryDependencies += "joda-time" % "joda-time" % "2.10.13"