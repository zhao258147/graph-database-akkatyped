lazy val `saga` = (project in file("saga"))
  .settings(Seq(
    name := "saga",
    credentials in ThisBuild += Credentials(Path.userHome / ".lightbend" / "commercial.credentials"),
    resolvers in ThisBuild += "lightbend-commercial-maven" at "https://repo.lightbend.com/commercial-releases",
    version := "0.1-SNAPSHOT",
    scalaVersion := "2.12.8",
    cinnamon in run := true,
    libraryDependencies ++= commonDependencies,
    libraryDependencies ++= Seq(
      Cinnamon.library.cinnamonAkka,
      Cinnamon.library.cinnamonAkkaPersistence,
      Cinnamon.library.cinnamonAkkaHttp,
      Cinnamon.library.cinnamonJvmMetricsProducer,
      Cinnamon.library.cinnamonPrometheus,
      Cinnamon.library.cinnamonPrometheusHttpServer,
      Cinnamon.library.cinnamonOpenTracing,
      Cinnamon.library.cinnamonOpenTracingJaeger
    ),
    dependencyOverrides += "com.google.guava" % "guava" % "19.0",
    PB.targets in Compile := Seq(
      scalapb.gen() -> (sourceManaged in Compile).value
    )
  )
).enablePlugins(Cinnamon)