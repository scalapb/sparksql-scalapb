import ReleaseTransformations._
import scalapb.compiler.Version.scalapbVersion

ThisBuild / organization := "com.thesamet.scalapb"

ThisBuild / scalacOptions ++= Seq("-deprecation", "-target:jvm-1.8")

ThisBuild / javacOptions ++= List("-target", "8", "-source", "8")

val Scala212 = "2.12.10"

lazy val sparkSqlScalaPB = project
  .in(file("sparksql-scalapb"))
  .settings(
    name := "sparksql-scalapb",
    crossScalaVersions := Seq(Scala212),
    libraryDependencies ++= Seq(
      "org.typelevel" %% "frameless-dataset" % "0.8.0",
      "com.thesamet.scalapb" %% "scalapb-runtime" % scalapbVersion,
      "com.thesamet.scalapb" %% "scalapb-runtime" % scalapbVersion % "protobuf",
      "org.apache.spark" %% "spark-sql" % "2.4.5" % "provided",
      "org.apache.spark" %% "spark-sql" % "2.4.5" % "test",
      "org.scalatest" %% "scalatest" % "3.1.2" % "test",
      "org.scalatestplus" %% "scalacheck-1-14" % "3.1.1.1" % "test",
      "com.github.alexarchambault" %% "scalacheck-shapeless_1.14" % "1.2.5" % "test"
    ),
    inConfig(Test)(
      sbtprotoc.ProtocPlugin.protobufConfigSettings
    ),
    PB.targets in Test := Seq(
      scalapb.gen(grpc = false) -> (sourceManaged in Test).value
    )
  )

publishTo in ThisBuild := sonatypePublishToBundle.value

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
  releaseStepCommandAndRemaining("+publishSigned"),
  releaseStepCommand("sonatypeBundleRelease"),
  setNextVersion,
  commitNextVersion,
  pushChanges
)

lazy val root =
  project
    .in(file("."))
    .settings(
      publishArtifact := false,
      publish := {},
      publishLocal := {}
    )
    .aggregate(
      sparkSqlScalaPB
    )
