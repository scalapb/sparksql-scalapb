addSbtPlugin("com.jsuereth" % "sbt-pgp" % "2.0.1")

addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.13")

addSbtPlugin("com.thesamet" % "sbt-protoc" % "0.99.30")

addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "3.9.2")

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.3.2")

libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.10.1"
