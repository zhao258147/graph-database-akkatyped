package com.example.saga.config

import com.example.graph.config.{NodeEntityParams, ReadSideConfig}
import com.example.user.config.UserEntityParams

import scala.concurrent.duration.FiniteDuration

case class SagaConfig (
  http: HttpConfig,
  readSideConfig: ReadSideConfig,
  cassandraConfig: CassandraConfig,
  nodeEntityParams: NodeEntityParams,
  userEntityParams: UserEntityParams
)

case class HttpConfig (
  interface: String,
  port: Int
)

case class CassandraConfig (
  contactPoints: String,
  port: Int,
  username: String,
  password: String
)