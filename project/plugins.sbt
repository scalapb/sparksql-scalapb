addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.1.2")

addSbtPlugin("com.github.sbt" % "sbt-release" % "1.1.0")

addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.4")

addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "3.9.10")

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.4")

libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.6"
