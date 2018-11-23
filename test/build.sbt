val pluginVersion = sys.props.get("plugin.version").getOrElse(
        sys.error("""|The system property 'plugin.version' is not defined.
                     |Specify this property by passing a version to SBT, for
                     |example -Dplugin.version=0.1.0-SNAPSHOT""".stripMargin
                 )
)

libraryDependencies ++= Seq(
    "com.thesamet.scalapb" %% "sparksql-scalapb" % pluginVersion,
    "org.apache.spark" %% "spark-sql" % "2.4.0",
    "org.scalatest" %% "scalatest" % "3.0.5" % "test"
)

PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value,
  scalapb.UdtGenerator -> (sourceManaged in Compile).value
)
