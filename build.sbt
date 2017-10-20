import ReleaseTransformations._

organization in ThisBuild := "com.trueaccord.scalapb"

//releaseCrossBuild := true
//
//releasePublishArtifactsAction := PgpKeys.publishSigned.value
//
//releaseProcess := Seq[ReleaseStep](
//  checkSnapshotDependencies,
//  inquireVersions,
//  runClean,
//  runTest,
//  setReleaseVersion,
//  commitReleaseVersion,
//  tagRelease,
//  ReleaseStep(action = Command.process("publishSigned", _), enableCrossBuild = true),
//  setNextVersion,
//  commitNextVersion,
//  pushChanges,
//  ReleaseStep(action = Command.process("sonatypeReleaseAll", _), enableCrossBuild = true)
//)

//testOptions in Test += Tests.Argument("-oD")

val scalaPbVersion = "0.6.6"

lazy val root = (project in file("."))
  .aggregate(plugin, library)
  .enablePlugins(CrossPerProjectPlugin)

lazy val plugin = (project in file("sparksql-scalapb-gen"))
  .enablePlugins(BuildInfoPlugin)
  .disablePlugins(sbtsparkpackage.SparkPackagePlugin)
  .settings(scriptedSettings: _*)
  .settings(
    name := "sparksql-scalapb-gen",
    sbtPlugin := true,
    scalaVersion := "2.10.6",
    libraryDependencies ++= Seq(
      "com.trueaccord.scalapb" %% "protoc-bridge" % "0.3.0-M1",
      "com.trueaccord.scalapb" %% "compilerplugin" % scalaPbVersion
    ),
    addSbtPlugin("com.thesamet" % "sbt-protoc" % "0.99.12"),
    scriptedLaunchOpts ++= Seq("-Dplugin.version=" + version.value),
    scriptedBufferLog := false,
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoPackage := "com.trueaccord.scalapb.sparksql.gen",
    publishLocal := publishLocal.dependsOn(publishLocal in library).value
  ).dependsOn(library)

lazy val library = project.in(file("sparksql-scalapb"))
  .settings(
    name := "sparksql-scalapb",
    spName := "trueaccord/sparksql-scalapb",
    crossScalaVersions ++= Seq("2.11.11", "2.10.6"),
    sparkVersion := "2.2.0",
    sparkComponents += "sql",
    spAppendScalaVersion := true,
    libraryDependencies ++= Seq(
      "com.trueaccord.scalapb" %% "scalapb-runtime" % scalaPbVersion,
      "org.scalatest" %% "scalatest" % "3.0.4" % "test"
    )
  )

