name := "hello"
version := "0.1"

// Spark 2.1.0 is the last version with support for scala 2.10
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.4" % "test"
