name := "sparkStreaming"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq("org.apache.spark" % "spark-sql_2.11" % "2.2.0",
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.0",
  "org.apache.spark" %% "spark-streaming" % "2.2.0",
  "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.2.0",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.1.0",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.+")
