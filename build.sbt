import _root_.sbtassembly.Plugin.AssemblyKeys
import _root_.sbtassembly.Plugin._
import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

assemblySettings

name := "od"

version := "1.0"

scalaVersion := "2.10.4"

javacOptions ++= Seq("-encoding", "UTF-8")

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-streaming_2.10" % "1.2.0-cdh5.3.0" % "provided",
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.2.0-cdh5.3.0" % "provided" ,
  "org.apache.spark" % "spark-core_2.10" % "1.2.0-cdh5.3.0" % "provided",
  "org.apache.hadoop" % "hadoop-common" % "2.5.0-cdh5.3.0" % "provided",
  "org.apache.hadoop" % "hadoop-client" % "2.5.0-mr1-cdh5.3.0" % "provided",
  "org.springframework" % "spring-jdbc" % "3.2.13.RELEASE",
  "com.alibaba" % "druid" % "1.0.12"
)

//resolvers +="internal" at "http://10.31.2.234:8081/nexus/content/groups/public/"

resolvers +="cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"