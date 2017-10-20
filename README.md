# sparksql-scalapb

This library provides utilities to work with ScalaPB in SparkSQL.

Adapted from: https://github.com/saurfang/sparksql-protobuf

## Using
just edit `project/plugins.sbt` to include:

    addSbtPlugin("com.trueaccord.scalapb" % "sparksql-scalapb-gen" % "0.1.9-SNAPSHOT")

This will automatically add all the needed dependencies and plugins related to scalapb.  You must still provide your own spark dependencies

## Testing
From the SBT shell, run:

    > ;reload ;scripted

## Building
From the SBT shell, run:

    > +publishLocal