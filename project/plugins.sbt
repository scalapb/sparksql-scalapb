addSbtPlugin("com.jsuereth" % "sbt-pgp" % "2.0.1")

addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.12")

addSbtPlugin("com.thesamet" % "sbt-protoc" % "0.99.27")

addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "3.8")

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.3.0")

libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.9.6"
