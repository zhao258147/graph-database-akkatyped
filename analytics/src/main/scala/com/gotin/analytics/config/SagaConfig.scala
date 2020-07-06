package com.gotin.analytics.config

import scala.concurrent.duration.FiniteDuration

case class AnalyticsConfig (
  http: HttpConfig,
  cassandraConfig: CassandraConfig,
  searchURL: String
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