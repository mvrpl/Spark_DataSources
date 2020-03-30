name := "DataSources"

version := "1.0"

scalaVersion := "2.11.12"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8") 

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.5" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5" % "provided"
libraryDependencies += "org.jsoup" % "jsoup" % "1.8.3"
libraryDependencies += "org.scala-lang.modules" %% "scala-xml" % "1.2.0"
libraryDependencies += "org.scalaj" %% "scalaj-http" % "2.4.2"
libraryDependencies += "org.tensorflow" % "tensorflow" % "1.5.0"
libraryDependencies += "org.tensorflow" % "proto" % "1.5.0"
libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.21"
libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.21"
libraryDependencies += "net.lingala.zip4j" % "zip4j" % "1.3.2"
libraryDependencies += "org.projectlombok" % "lombok" % "1.18.2" % "provided"

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.protobuf.**" -> "shadedproto.@1").inAll
)

compileOrder := CompileOrder.JavaThenScala

enablePlugins(ProtobufPlugin)