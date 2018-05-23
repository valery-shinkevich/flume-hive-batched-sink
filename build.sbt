organization := "org.apache.flume.flume-ng-sinks"

name := "flume-hive-batched-sink"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-reflect" % "2.11.8",
  "org.apache.flume" % "flume-ng-core" % "1.8.0",
  "org.apache.flume" % "flume-ng-configuration" % "1.8.0" ,
  "org.apache.flume" % "flume-ng-sdk" % "1.8.0",

  "org.apache.hadoop" % "hadoop-common" % "2.4.0",
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.4.0",

  "org.apache.hive.hcatalog" % "hive-hcatalog-streaming" % "0.13.0",
  "org.apache.hive.hcatalog" % "hive-hcatalog-core" % "0.13.0",
  "org.apache.hive" % "hive-serde" % "0.13.0",
  "org.apache.hive" % "hive-cli" % "0.13.0",
  "org.apache.zookeeper" % "zookeeper" % "3.4.5",

  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
  "com.sun.jersey" % "jersey-client" % "1.8"
)

publishMavenStyle := true
autoScalaLibrary := true
