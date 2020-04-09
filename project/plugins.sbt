addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.4.0")
addSbtPlugin("com.lightbend.cinnamon" % "sbt-cinnamon" % "2.13.1")
addSbtPlugin("com.thesamet" % "sbt-protoc" % "0.99.28")
libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.9.6"

resolvers += Resolver.url("lightbend-commercial", url("https://repo.lightbend.com/commercial-releases"))(Resolver.ivyStylePatterns)