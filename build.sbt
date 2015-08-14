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
//  "org.apache.spark" % "spark-streaming_2.10" % "1.2.0-cdh5.3.0" % "provided",
//  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.2.0-cdh5.3.0" % "provided" ,
//  "org.apache.spark" % "spark-core_2.10" % "1.2.0-cdh5.3.0" % "provided",
  "org.apache.hadoop" % "hadoop-common" % "2.5.0-cdh5.3.0"  %"provided",
  "org.apache.hadoop" % "hadoop-client" % "2.5.0-mr1-cdh5.3.0" % "provided",
  "org.springframework" % "spring-jdbc" % "3.2.13.RELEASE",
  "com.alibaba" % "druid" % "1.0.12",
  "com.google.guava" % "guava" % "18.0",
  "org.geotools" % "gt-shapefile" % "13.2" exclude("com.vividsolutions", "jts"),
  "com.vividsolutions" % "jts" % "1.8"
)

//resolvers +="internal" at "http://10.31.2.234:8081/nexus/content/groups/public/"

resolvers +="cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

//resolvers +="m2" at "http://download.java.net/maven/2"

resolvers +="geo" at "http://download.osgeo.org/webdav/geotools/"

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
{
  case PathList("META-INF", "spring.tooling") => MergeStrategy.first
  case x => old(x)
}
}