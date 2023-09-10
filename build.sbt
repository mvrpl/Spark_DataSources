name := "DataSources"

version := "1.0"

scalaVersion := "2.13.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.4.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.4.1" % "provided"
libraryDependencies += "com.lihaoyi" %% "requests" % "0.8.0"
libraryDependencies += "com.jayway.jsonpath" % "json-path" % "2.8.0"
libraryDependencies += "org.scala-lang" % "scala-compiler" % scalaVersion.value
libraryDependencies += "io.github.hakky54" % "sslcontext-kickstart" % "8.1.6"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}