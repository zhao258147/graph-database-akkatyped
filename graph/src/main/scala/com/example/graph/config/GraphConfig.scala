package com.example.graph.config

import scala.concurrent.duration.FiniteDuration

case class GraphConfig (
  http: HttpConfig,
  readSideConfig: ReadSideConfig,
  cassandraConfig: CassandraConfig,
  nodeEntityParams: NodeEntityParams
)

case class HttpConfig (
  interface: String,
  port: Int
)

case class ReadSideConfig (
  producerParallelism: Int,
  idleTimeout: FiniteDuration
)

case class CassandraConfig (
  contactPoints: String,
  port: Int,
  username: String,
  password: String
)

case class NodeEntityParams (
  numberOfRecommendationsToTake: Int,
  numberOfSimilarUsersToTake: Int,
  similarUserEdgeWeightFilter: Int,
  clickRecorderIncrementalClicks: Int,
  clickRecorderIncrementalTimeMS: Int,
  edgeWeightFilter: Int,
  numberOfUniqueVisitorsToKeep: Int,
  numberOfUpdatedVisitorLabelsToKeep: Int
)