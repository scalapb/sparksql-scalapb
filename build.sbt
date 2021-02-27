import ReleaseTransformations._
import scalapb.compiler.Version.scalapbVersion

ThisBuild / organization := "com.thesamet.scalapb"

ThisBuild / scalacOptions ++= Seq("-deprecation", "-target:jvm-1.8")

ThisBuild / javacOptions ++= List("-target", "8", "-source", "8")

val Scala212 = "2.12.13"

lazy val sparkSqlScalaPB = project
  .in(file("sparksql-scalapb"))
  .settings(
    name := "sparksql-scalapb",
    crossScalaVersions := Seq(Scala212),
    libraryDependencies ++= Seq(
      "org.typelevel" %% "frameless-dataset" % "0.10.1",
      "com.thesamet.scalapb" %% "scalapb-runtime" % scalapbVersion,
      "com.thesamet.scalapb" %% "scalapb-runtime" % scalapbVersion % "protobuf",
      "org.apache.spark" %% "spark-sql" % "3.1.0" % "provided",
      "org.apache.spark" %% "spark-sql" % "3.1.0" % "test",
      "org.scalatest" %% "scalatest" % "3.2.4" % "test",
      "org.scalatestplus" %% "scalacheck-1-14" % "3.2.2.0" % "test",
      "com.github.alexarchambault" %% "scalacheck-shapeless_1.14" % "1.2.5" % "test"
    ),
    inConfig(Test)(
      sbtprotoc.ProtocPlugin.protobufConfigSettings
    ),
    Test / PB.targets := Seq(
      scalapb.gen(grpc = false) -> (Test / sourceManaged).value
    ),
    Test / run / fork := true
  )

ThisBuild / publishTo := sonatypePublishToBundle.value

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
