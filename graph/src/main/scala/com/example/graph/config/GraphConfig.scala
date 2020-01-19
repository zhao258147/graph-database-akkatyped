package com.example.graph.config

import scala.concurrent.duration.FiniteDuration

case class GraphConfig (
  http: HttpConfig,
  readSideConfig: ReadSideConfig
)

case class HttpConfig (
  interface: String,
  port: Int
)

case class ReadSideConfig (
  producerParallelism: Int,
  idleTimeout: FiniteDuration
)