import ReleaseTransformations._
import scalapb.compiler.Version.scalapbVersion

organization in ThisBuild := "com.thesamet.scalapb"

val Scala210 = "2.10.7"

val Scala211 = "2.11.12"

val Scala212 = "2.12.7"

lazy val udtGenerator = project.in(file("sparksql-scalapb-gen"))
  .enablePlugins(ScriptedPlugin)
  .settings(
    name := "sparksql-scalapb-gen",
    crossScalaVersions := Seq(Scala210, Scala212),
    libraryDependencies ++= Seq(
      "com.thesamet.scalapb" %% "protoc-bridge" % "0.7.3",
      "com.thesamet.scalapb" %% "compilerplugin" % scalapbVersion
    ),
    name := "sparksql-scalapb-gen",
    PB.targets in Compile := Seq(),
    scriptedLaunchOpts := { scriptedLaunchOpts.value ++
      Seq("-Xmx1024M", "-Dplugin.version=" + version.value)
    },
    scriptedBufferLog := false
  )

lazy val sparkSqlScalaPB = project.in(file("sparksql-scalapb"))
  .settings(
    name := "sparksql-scalapb",
    crossScalaVersions := Seq(Scala211, Scala212),
    libraryDependencies ++= Seq(
      "com.thesamet.scalapb" %% "scalapb-runtime" % scalapbVersion,
      "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided"
    )
  )

publishTo in ThisBuild := sonatypePublishTo.value

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

lazy val root =
  project.in(file("."))
    .settings(
      publishArtifact := false,
      publish := {},
      publishLocal := {}
    ).aggregate(
      sparkSqlScalaPB, udtGenerator
    )

