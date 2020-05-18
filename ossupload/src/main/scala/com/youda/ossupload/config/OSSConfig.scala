package com.youda.ossupload.config

case class OSSConfig(
  http: HttpConfig,
  rootPath: String,
  bucketName: String,
  endpoint: String,
  accessKeyIdL: String,
  accessKeySecret: String
)

case class HttpConfig (
  interface: String,
  port: Int
)
