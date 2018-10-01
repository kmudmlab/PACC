name := "pacc"

version := "0.1"

val versions = Map(
  "hadoop" -> "2.7.3",
  "spark" -> "2.2.0",
  "scala" -> "2.11.8",
  "logback" -> "1.1.9",
  "fastutil" -> "8.2.1",
  "scala-logging" -> "3.5.0",
  "scalatest" -> "3.0.1",
  "junit" -> "4.12",
  "junit-interface" -> "0.11",
  "trove" -> "3.0.3",
  "kryo" -> "3.0.2",
  "scopt" -> "3.5.0",
  "primitive" -> "1.2.1"
)


scalaVersion := versions("scala")

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % versions("hadoop") % "provided",
  "it.unimi.dsi" % "fastutil" % versions("fastutil")
)

//for spark
libraryDependencies ++= Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % versions("scala-logging"),
  "org.apache.spark" %% "spark-core" % versions("spark") % "provided",
  "org.scalatest" %% "scalatest" % versions("scalatest"),
  "com.github.scopt" %% "scopt" % versions("scopt")
)

//skip test during assembly
test in assembly := {}

target in assembly := baseDirectory.value / "bin"
assemblyJarName in assembly := name.value + "-" + version.value + ".jar"

//remove main-class
packageOptions in assembly ~= { os => os filterNot {_.isInstanceOf[Package.MainClass]} }