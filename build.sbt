name := "DataSources"

version := "1.0"

scalaVersion := "2.13.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.4.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.4.1" % "provided"
libraryDependencies += "com.lihaoyi" %% "requests" % "0.8.0"

/* assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
} */