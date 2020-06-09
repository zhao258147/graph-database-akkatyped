package com.youda.ossupload.config

case class OSSConfig(
  http: HttpConfig,
  rootPath: String,
  bucketName: String,
  endpoint: String,
  accessKeyId: String,
  accessKeySecret: String,
  salt: String
)

case class HttpConfig (
  interface: String,
  port: Int
)
