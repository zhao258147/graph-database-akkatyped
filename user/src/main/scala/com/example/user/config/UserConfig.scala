package com.example.user.config

import scala.concurrent.duration.FiniteDuration

case class UserConfig (
  http: HttpConfig,
  readSideConfig: ReadSideConfig,
  cassandraConfig: CassandraConfig
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