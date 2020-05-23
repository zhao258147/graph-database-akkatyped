# graph-database-akkatyped

to run locally
- sbt graph/run
- sbt user/run
- sbt saga/run

to package into docker images
- sbt graph/docker:publishLocal
- sbt user/docker:publishLocal
- sbt saga/docker:publishLocal
