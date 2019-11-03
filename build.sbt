import ReleaseTransformations._
import scalapb.compiler.Version.scalapbVersion

organization in ThisBuild := "com.thesamet.scalapb"

val Scala210 = "2.10.7"

val Scala211 = "2.11.12"

val Scala212 = "2.12.10"

lazy val sparkSqlScalaPB = project
  .in(file("sparksql-scalapb"))
  .settings(
    name := "sparksql-scalapb",
    crossScalaVersions := Seq(Scala211, Scala212),
    libraryDependencies ++= Seq(
      "com.thesamet.scalapb" %% "scalapb-runtime" % scalapbVersion,
      "org.apache.spark" %% "spark-sql" % "2.4.4" % "provided"
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
      sparkSqlScalaPB,
    )
