package scalapb

import sbt.{AutoPlugin, inConfig, _}
import Keys._
import sbtprotoc.ProtocPlugin
import sbtprotoc.ProtocPlugin.autoImport.PB
import sbtprotoc.ProtocPlugin.protobufConfigSettings
/**
  * Created 10/19/17 8:31 PM
  * Author: ljacobsen
  *
  * This code is copyright (C) 2017 ljacobsen
  *
  */
object SparkSQLScalaPBPlugin extends AutoPlugin {

  override def trigger: PluginTrigger = allRequirements

  object autoImport {
    /* Whatever you want brought into scope automatically for users of your plugin */
  }

  override def projectSettings = ProtocPlugin.projectSettings ++ Seq(
    /* Whatever settings/behavior you want to make available to the project that includes your plugin */
    libraryDependencies += "com.trueaccord.scalapb" %% "sparksql-scalapb" % com.trueaccord.scalapb.sparksql.gen.BuildInfo.version,
    PB.targets in Compile := Seq(
      scalapb.gen() -> (sourceManaged in Compile).value,
      new scalapb.UdtGenerator -> (sourceManaged in Compile).value,
      new scalapb.SqlSourceGenerator -> (sourceManaged in Compile).value
    )
  )



}
