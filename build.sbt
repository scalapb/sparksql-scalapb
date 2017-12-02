import ReleaseTransformations._

scalaVersion in ThisBuild := "2.11.8"

crossScalaVersions in ThisBuild := Seq("2.11.8", "2.10.5")

organization in ThisBuild := "com.thesamet.scalapb"

scalacOptions in ThisBuild ++= {
  CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, v)) if v <= 11 => List("-target:jvm-1.7")
    case _ => Nil
  }
}

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

val scalaPbVersion = "0.7.0-rc1"

lazy val sparkSqlScalaPB = project.in(file("sparksql-scalapb"))
  .settings(
    name := "sparksql-scalapb",

    spName := "scalapb/sparksql-scalapb",

    sparkVersion := "2.2.0",

    sparkComponents += "sql",

    spAppendScalaVersion := true,

    libraryDependencies ++= Seq(
      "com.thesamet.scalapb" %% "scalapb-runtime" % scalaPbVersion,
      "org.scalatest" %% "scalatest" % "3.0.1" % "test"
    ),
    inConfig(Test)(sbtprotoc.ProtocPlugin.protobufConfigSettings),
    PB.targets in Compile := Seq(),
    PB.targets in Test := Seq(
      scalapb.gen() -> (sourceManaged in Test).value
      // scalapb.UdtGenerator -> (sourceManaged in Test).value
    )
  )

testOptions in Test += Tests.Argument("-oD")

lazy val udtGenerator = project.in(file("sparksql-scalapb-gen"))
  .disablePlugins(sbtsparkpackage.SparkPackagePlugin)
  .settings(
    libraryDependencies ++= Seq(
      "com.thesamet.scalapb" %% "protoc-bridge" % "0.7.0",
      "com.thesamet.scalapb" %% "compilerplugin" % scalaPbVersion
    ),
    name := "sparksql-scalapb-gen",
    PB.targets in Compile := Seq()
  )

lazy val root =
  project.in(file("."))
    .settings(
      publishArtifact := false,
      publish := {},
      publishLocal := {}
    ).aggregate(
      sparkSqlScalaPB, udtGenerator)
