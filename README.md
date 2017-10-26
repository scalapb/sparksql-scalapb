# sparksql-scalapb

This library provides utilities to work with ScalaPB in SparkSQL.

Adapted from: https://github.com/saurfang/sparksql-protobuf

_This library only supports Spark 2.2.0+ due to interface changes in Spark from version 2.1 to 2.2.  If you need to support Spark 2.1 or lower, please use a version 1.8 of this project._

## Features
* reading and saving delimeted protobuf files to/from a spark sql dataframe
* enabling enum as user defined types in spark sql 

For examples, please see [DataSpec.scala](sparksql-scalapb-gen/src/sbt-test/sparksql-scalapb-tests/simple/src/test/scala/DataSpec.scala)

## Using
edit `project/plugins.sbt` to include:

    addSbtPlugin("com.thesamet" % "sbt-protoc" % "0.99.12")
    libraryDependencies += "com.trueaccord.scalapb" %% "compilerplugin" % "0.6.6"
    libraryDependencies += "com.trueaccord.scalapb" %% "sparksql-scalapb-gen" % [this project version]

edit `build.sbt` to include:

    libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.4" % "test"
    libraryDependencies += "com.trueaccord.scalapb" %% "sparksql-scalapb" % [this project version]
       
    PB.targets in Compile := Seq(
      scalapb.gen() -> (sourceManaged in Compile).value,
      new scalapb.UdtGenerator -> (sourceManaged in Compile).value,
      new scalapb.SqlSourceGenerator -> (sourceManaged in Compile).value
    )

## Testing
From the SBT shell, run:

    > ;reload ;scripted

## Building Locally
From the SBT shell, run:

    > +publishLocal