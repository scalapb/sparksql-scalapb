addSbtPlugin("com.thesamet" % "sbt-protoc" % "0.99.12")
libraryDependencies += "com.trueaccord.scalapb" %% "compilerplugin" % "0.6.6"

{
  sys.props.get("plugin.version") match {
    case Some(pluginVersion) =>
      libraryDependencies += ("com.trueaccord.scalapb" %% "sparksql-scalapb-gen" % pluginVersion)
    case None =>
      sys.error(
        """
          |The system property 'plugin.version' is not defined.
          |Specify this property using the scriptedLaunchOpts -D.
        """.stripMargin.trim
      )
  }
}