name := "hello"
version := "0.1"

// Spark 2.1.0 is the last version with support for scala 2.10
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.4" % "test"

{
  sys.props.get("plugin.version") match {
    case Some(pluginVersion) =>
      libraryDependencies += ("com.trueaccord.scalapb" %% "sparksql-scalapb" % pluginVersion)
    case None =>
      sys.error(
        """
          |The system property 'plugin.version' is not defined.
          |Specify this property using the scriptedLaunchOpts -D.
        """.stripMargin.trim
      )
  }
}

PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value,
  new scalapb.UdtGenerator -> (sourceManaged in Compile).value,
  new scalapb.SqlSourceGenerator -> (sourceManaged in Compile).value
)
