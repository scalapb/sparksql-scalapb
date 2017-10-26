import ReleaseTransformations._
import sbtsparkpackage.SparkPackagePlugin.autoImport.sparkVersion

organization in ThisBuild := "com.trueaccord.scalapb"

releaseCrossBuild := true

releasePublishArtifactsAction := PgpKeys.publishSigned.value

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  ReleaseStep(action = Command.process("publishSigned", _), enableCrossBuild = true),
  setNextVersion,
  commitNextVersion,
  pushChanges,
  ReleaseStep(action = Command.process("sonatypeReleaseAll", _), enableCrossBuild = true)
)

testOptions in Test += Tests.Argument("-oD")

val scalaPbVersion = "0.6.6"

lazy val root = (project in file("."))
  .aggregate(library, `library-codegen`)
  .enablePlugins(CrossPerProjectPlugin)

lazy val `library-codegen` = (project in file("sparksql-scalapb-gen"))
  .settings(scriptedSettings: _*)
  .settings(
    name := "sparksql-scalapb-gen",
    scalaVersion := "2.10.6",
    crossScalaVersions := Seq("2.10.6","2.11.11"),
    libraryDependencies ++= Seq(
      "com.trueaccord.scalapb" %% "protoc-bridge" % "0.3.0-M1",
      "com.trueaccord.scalapb" %% "compilerplugin" % scalaPbVersion,
      "com.trueaccord.scalapb" %% "scalapb-runtime" % scalaPbVersion
    ),
    scriptedLaunchOpts ++= Seq("-Dplugin.version=" + version.value),
    scriptedBufferLog := false,
    publishLocal := publishLocal.dependsOn(publishLocal in library).value
  )


lazy val library = project.in(file("sparksql-scalapb"))
  .settings(
    name := "sparksql-scalapb",
    spName := "trueaccord/sparksql-scalapb",
    scalaVersion := "2.11.11",
    sparkVersion := "2.2.0",
    sparkComponents += "sql",
    spAppendScalaVersion := true,
    libraryDependencies ++= Seq(
      "com.trueaccord.scalapb" %% "scalapb-runtime" % scalaPbVersion,
      "org.scalatest" %% "scalatest" % "3.0.4" % "test"
    )
  )

