ThisBuild / organization := "com.thesamet.scalapb"

ThisBuild / scalacOptions ++= Seq("-deprecation", "-target:jvm-1.8")

ThisBuild / javacOptions ++= List("-target", "8", "-source", "8")

Global / concurrentRestrictions := Seq(
  Tags.limit(Tags.Test, 1)
)

val Scala212 = "2.12.15"

val Scala213 = "2.13.8"

lazy val Spark32 = Spark("3.2.1")

lazy val Spark31 = Spark("3.1.2")

lazy val Spark30 = Spark("3.0.3")

lazy val ScalaPB0_11 = ScalaPB("0.11.8")

lazy val ScalaPB0_10 = ScalaPB("0.10.11")

lazy val framelessDatasetName = settingKey[String]("frameless-dataset-name")

lazy val spark = settingKey[Spark]("spark")

lazy val scalapb = settingKey[ScalaPB]("scalapb")

def scalapbPlugin(version: String) =
  protocbridge.SandboxedJvmGenerator.forModule(
    s"scala${version}",
    protocbridge.Artifact(
      "com.thesamet.scalapb",
      "compilerplugin_2.12",
      version
    ),
    "scalapb.ScalaPbCodeGenerator$",
    Nil
  )

lazy val `sparksql-scalapb` = (projectMatrix in file("sparksql-scalapb"))
  .defaultAxes()
  .settings(
    libraryDependencies ++= Seq(
      "org.typelevel" %% framelessDatasetName.value % "0.11.1",
      "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.value.scalapbVersion,
      "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.value.scalapbVersion % "protobuf",
      "org.apache.spark" %% "spark-sql" % spark.value.sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql" % spark.value.sparkVersion % "test",
      "org.scalatest" %% "scalatest" % "3.2.11" % "test",
      "org.scalatestplus" %% "scalacheck-1-14" % "3.2.2.0" % "test",
      "com.github.alexarchambault" %% "scalacheck-shapeless_1.15" % "1.3.0" % "test"
    ),
    spark := {
      virtualAxes.value
        .collectFirst { case x: Spark =>
          x
        }
        .getOrElse(
          throw new RuntimeException(
            s"Could not find Spark version in virtualAxes. Got $virtualAxes"
          )
        )
    },
    scalapb := {
      virtualAxes.value
        .collectFirst { case x: ScalaPB =>
          x
        }
        .getOrElse(
          throw new RuntimeException(
            s"Could not find ScalaPB version in virtualAxes. Got $virtualAxes"
          )
        )
    },
    framelessDatasetName := {
      spark.value match {
        case Spark32 => "frameless-dataset"
        case Spark31 => "frameless-dataset-spark31"
        case Spark30 => "frameless-dataset-spark30"
        case _       => ???
      }
    },
    name := s"sparksql${spark.value.majorVersion}${spark.value.minorVersion}-${scalapb.value.idSuffix}",
    inConfig(Test)(
      sbtprotoc.ProtocPlugin.protobufConfigSettings
    ),
    Test / PB.targets := Seq(
      scalapbPlugin(scalapb.value.scalapbVersion) -> (Test / sourceManaged).value
    ),
    Test / run / fork := true
  )
  .customRow(
    scalaVersions = Seq(Scala212, Scala213),
    axisValues = Seq(Spark32, ScalaPB0_11, VirtualAxis.jvm),
    settings = Seq()
  )
  .customRow(
    scalaVersions = Seq(Scala212),
    axisValues = Seq(Spark31, ScalaPB0_11, VirtualAxis.jvm),
    settings = Seq()
  )
  .customRow(
    scalaVersions = Seq(Scala212),
    axisValues = Seq(Spark30, ScalaPB0_11, VirtualAxis.jvm),
    settings = Seq()
  )
  .customRow(
    scalaVersions = Seq(Scala212, Scala213),
    axisValues = Seq(Spark32, ScalaPB0_10, VirtualAxis.jvm),
    settings = Seq()
  )
  .customRow(
    scalaVersions = Seq(Scala212),
    axisValues = Seq(Spark31, ScalaPB0_10, VirtualAxis.jvm),
    settings = Seq()
  )
  .customRow(
    scalaVersions = Seq(Scala212),
    axisValues = Seq(Spark30, ScalaPB0_10, VirtualAxis.jvm),
    settings = Seq()
  )

ThisBuild / publishTo := sonatypePublishToBundle.value

lazy val root =
  project
    .in(file("."))
    .settings(
      publishArtifact := false,
      publish := {},
      publishLocal := {}
    )
    .aggregate(
      `sparksql-scalapb`.projectRefs: _*
    )
