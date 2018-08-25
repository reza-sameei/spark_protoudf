
name := "spark-protoudf"

// libraryDependencies += "org.scala-lang" %% "scala-reflect" % scalaVersion.value
libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5"

enablePlugins(ProtobufPlugin)

test := { clean.value; (test in Test).value }
