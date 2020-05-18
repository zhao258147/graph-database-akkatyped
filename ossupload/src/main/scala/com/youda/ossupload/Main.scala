package com.youda.ossupload

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorSystem, Behavior}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.Multipart
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import com.aliyun.oss.{OSS, OSSClientBuilder}
import com.typesafe.config.ConfigFactory
import com.youda.ossupload.config.OSSConfig
import com.youda.ossupload.http.{CORSHandler, RequestApi}
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._

import scala.concurrent.ExecutionContextExecutor

object Main extends App {
  import akka.actor.typed.scaladsl.adapter._

  val conf = ConfigFactory.load()
  val config = conf.as[OSSConfig]("OSSConfig")

  implicit val typedSystem: ActorSystem[OSSUploadCommand] = ActorSystem(Main(), "RayDemo")

  implicit val system = typedSystem.toClassic
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContextExecutor = system.dispatcher

  // 阿里云主账号AccessKey拥有所有API的访问权限，风险很高。强烈建议您创建并使用RAM账号进行API访问或日常运维，请登录 https://ram.console.aliyun.com 创建RAM账号。
  // 创建OSSClient实例。
  val ossClient: OSS = new OSSClientBuilder().build(config.endpoint, config.accessKeyId, config.accessKeySecret)

//  Multipart.FormData.BodyPart.Strict()
  val route: Route = RequestApi.route(ossClient, config)

  private val cors = new CORSHandler {}

  Http().bindAndHandle(
    cors.corsHandler(route),
    config.http.interface,
    config.http.port
  )

  sealed trait OSSUploadCommand
  case class OSSUpload() extends OSSUploadCommand

  def apply(): Behavior[OSSUploadCommand] = Behaviors.setup{ context =>
    Behaviors.receiveMessage{
      case OSSUpload() =>
        Behaviors.same
    }
  }

  scala.sys.addShutdownHook{
    ossClient.shutdown()
  }


}
