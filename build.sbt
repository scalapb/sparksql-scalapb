import ReleaseTransformations._

scalaVersion := "2.11.8"

crossScalaVersions := Seq("2.11.8", "2.10.5")

organization in ThisBuild := "com.trueaccord.scalapb"

name in ThisBuild := "sparksql-scalapb"

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

val scalaPbVersion = "0.5.33"

spName := "trueaccord/sparksql-scalapb"

sparkVersion := "2.0.0"

sparkComponents += "sql"

spAppendScalaVersion := true

libraryDependencies ++= Seq(
  "com.trueaccord.scalapb" %% "scalapb-runtime" % scalaPbVersion
)

