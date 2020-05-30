package com.youda.ossupload.http

import java.io.{BufferedReader, InputStreamReader}
import java.nio.file.{Path, Paths}

import akka.actor.typed.ActorSystem
import akka.http.scaladsl.model.{HttpRequest, StatusCodes}
import akka.http.scaladsl.server.Directives.{complete, pathPrefix, _}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.directives.{Credentials, DebuggingDirectives, LoggingMagnet}
import akka.stream.scaladsl.{FileIO, Source}
import akka.stream.{ActorMaterializer, IOResult}
import akka.util.{ByteString, Timeout}
import com.aliyun.oss.OSS
import com.aliyun.oss.model.OSSObjectSummary
import com.youda.ossupload.config.OSSConfig
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.json4s.{DefaultFormats, Formats}
import org.json4s.jackson.Serialization.write

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import collection.JavaConverters._

object RequestApi extends Json4sSupport {
  implicit val timeout: Timeout = 40 minutes
  implicit val formats: Formats = DefaultFormats

  def myUserPassAuthenticator(credentials: Credentials, companyName: String): Option[String] =
    credentials match {
      case p @ Credentials.Provided(id) if p.verify("p4ssw0rd") =>
        Some(id)
      case p @ Credentials.Provided(id) if p.verify(id) =>
        Some(id)
      case x =>
        println(x)
        None
    }

  def upload(ossClient: OSS, path: Path, fileStream: Source[ByteString, Any], objectName: String, bucketName: String)(implicit materializer: ActorMaterializer, ec: ExecutionContext): Try[Future[String]] = {
    val tryFile = Try {
      println(path.getParent)
      println(path)
      val file = path.toFile
      println(file)
      if(file.exists()) {
        file.delete()
      } else {
        println(path.getParent.toFile.mkdirs())
      }
      path
    }

    tryFile.map{ path =>
      fileStream.map{ s =>
        println(s.size)
        s
      }.runWith(
        FileIO.toPath(path)
      ).map((ioresult: IOResult) =>
        ioresult.status
      ).map{ x =>
        println("status"*10)
        println(x)
        ossClient.putObject(bucketName, objectName, path.toFile).getETag
      }
    }
  }

  case class ContentJson(
    title: String,
    primaryTag: String,
    secondaryTags: Seq[String],
    shortDescription: String,
    longDescription: String
  )
  val ContentJsonFileName = "_contentjson"


  private def companyJsonRoute(
    ossClient: OSS,
    config: OSSConfig,
    companyName: String
  )(implicit system: ActorSystem[Nothing], ec: ExecutionContext, materializer: ActorMaterializer): Route = Route {
    path("json") {
      println("company json")
      get {
        val objectName = s"$companyName/$ContentJsonFileName"
        println(objectName)
        val getObject = Try {
          val ossObject = ossClient.getObject(config.bucketName, objectName)

          val reader = new BufferedReader(new InputStreamReader(ossObject.getObjectContent()))
          val stringBuilder = new StringBuilder
          var line = reader.readLine()

          while (line != null) {
            println(line)
            stringBuilder.append(line)
            line = reader.readLine()
          }
          stringBuilder.mkString
        }

        getObject match {
          case Success(value) =>
            println(value)
            complete(value)
          case Failure(exception) =>
            println(exception)
            complete(StatusCodes.NoContent)
        }
      } ~
      post {
        entity(as[String]) { contentJson =>
          println(contentJson)
          val objectName = s"$companyName/$ContentJsonFileName"
          println(objectName)
          val path: Path = Paths.get(s"${config.rootPath}/$companyName/$ContentJsonFileName")
          val fileStream = Source.single(contentJson).map(str => ByteString(str))

          upload(ossClient, path, fileStream, objectName, config.bucketName) match {
            case Success(value) =>
              complete(value)
            case Failure(exception) =>
              println(exception)
              complete(StatusCodes.BadGateway)
          }
        }
      }
    }
  }

  private def companyUploadRoute(
    ossClient: OSS,
    config: OSSConfig,
    companyName: String
  )(implicit system: ActorSystem[Nothing], ec: ExecutionContext, materializer: ActorMaterializer): Route = Route {
    path("logo") {
      fileUpload("logo") {
        case (fileInfo, fileStream: Source[ByteString, Any]) =>
          println(fileInfo)
          val objectName = s"$companyName/${fileInfo.fieldName}"

          val path: Path = Paths.get(s"${config.rootPath}/$companyName/${fileInfo.fileName}")

          upload(ossClient, path, fileStream, objectName, config.bucketName) match {
            case Success(value) =>
              complete(value)
            case Failure(exception) =>
              println(exception)
              complete(StatusCodes.BadGateway)
          }
      }
    } ~
    path("banner") {
      fileUpload("banner") {
        case (fileInfo, fileStream: Source[ByteString, Any]) =>
          println(fileInfo)
          val objectName = s"$companyName/${fileInfo.fieldName}"
          println(objectName)
          val path: Path = Paths.get(s"${config.rootPath}/$companyName/${fileInfo.fileName}")

          upload(ossClient, path, fileStream, objectName, config.bucketName) match {
            case Success(value) =>
              complete(value)
            case Failure(exception) =>
              println(exception)
              complete(StatusCodes.BadGateway)
          }
      }
    } ~
    path("list") {
      get {
        val objectListing = ossClient.listObjects(config.bucketName, companyName)
        val sums = objectListing.getObjectSummaries().asScala.toList
        val contents = sums.map { summary =>
          println(summary.getKey)
          val tokens: Array[String] = summary.getKey.split("/")
          tokens.tail
        }.collect {
          case path if path.size > 1 =>
            path.headOption
        }.flatten.toSet

        complete(write(contents))
      }
    }
  }

  private def deleteContentRoute(ossClient: OSS,
    config: OSSConfig,
    companyName: String,
    contentName: String)(implicit system: ActorSystem[Nothing], ec: ExecutionContext, materializer: ActorMaterializer): Route =
      delete {
        val objectListing = ossClient.listObjects(config.bucketName, companyName)
        val sums = objectListing.getObjectSummaries().asScala.toList
        val contents: Seq[OSSObjectSummary] = sums.filter(_.getKey.contains(contentName))
        val deletes = contents.map(x => Try(ossClient.deleteObject(config.bucketName, x.getKey)))
        contents.foreach(x => println(x.getKey))
        deletes.foreach(x => println(x.failed))
        if(deletes.exists(_.isFailure)){
          complete(StatusCodes.BadGateway, "cannot remove all items")
        } else
        complete(StatusCodes.OK)
      }

  private def contentJsonRoute(
    ossClient: OSS,
    config: OSSConfig,
    companyName: String,
    contentName: String
  )(implicit system: ActorSystem[Nothing], ec: ExecutionContext, materializer: ActorMaterializer): Route = path("json") {
    get {
      val objectName = s"$companyName/$contentName/$ContentJsonFileName"
      val getObject = Try {
        val ossObject = ossClient.getObject(config.bucketName, objectName)

        val reader = new BufferedReader(new InputStreamReader(ossObject.getObjectContent()))
        val stringBuilder = new StringBuilder
        var line = reader.readLine()

        while (line != null) {
          stringBuilder.append(line)
          line = reader.readLine()
        }
        stringBuilder.mkString
      }
      getObject match {
        case Success(value) =>
          complete(value)
        case Failure(exception) =>
          println(exception)
          complete(StatusCodes.NoContent)
      }

    } ~
    post {
      entity(as[String]) { contentJson =>
        println(contentJson)
        val objectName = s"$companyName/$contentName/$ContentJsonFileName"
        println(objectName)
        val path: Path = Paths.get(s"${config.rootPath}/$companyName/$contentName/$ContentJsonFileName")
        val fileStream = Source.single(contentJson).map(str => ByteString(str))

        upload(ossClient, path, fileStream, objectName, config.bucketName) match {
          case Success(value) =>
            complete(value)
          case Failure(exception) =>
            println(exception)
            complete(StatusCodes.BadGateway)
        }
      }
    }
  }



  private def contentUploadRoute(
    ossClient: OSS,
    config: OSSConfig,
    companyName: String,
    contentName: String
  )(implicit system: ActorSystem[Nothing], ec: ExecutionContext, materializer: ActorMaterializer): Route = Route {
    path("cover") {
      fileUpload("cover") {
        case (fileInfo, fileStream: Source[ByteString, Any]) =>
          println(fileInfo)
          val objectName = s"$companyName/$contentName/${fileInfo.fieldName}"
          println(objectName)
          val path: Path = Paths.get(s"${config.rootPath}/$companyName/$contentName/${fileInfo.fileName}")

          upload(ossClient, path, fileStream, objectName, config.bucketName) match {
            case Success(value) =>
              complete(value)
            case Failure(exception) =>
              println(exception)
              complete(StatusCodes.BadGateway)
          }
      }
    } ~
    path("content") {
      fileUpload("content") {
        case (fileInfo, fileStream: Source[ByteString, Any]) =>
          println(fileInfo)
          val lastDot = fileInfo.fileName.lastIndexOf('.')
          val ext = fileInfo.fileName.substring(lastDot, fileInfo.fileName.size)
          println(ext)
          val objectName = s"$companyName/$contentName/content$ext"
          println(objectName)
          val path: Path = Paths.get(s"${config.rootPath}/$companyName/$contentName/${fileInfo.fileName}")

          upload(ossClient, path, fileStream, objectName, config.bucketName) match {
            case Success(value) =>
              complete(value)
            case Failure(exception) =>
              println(exception)
              complete(StatusCodes.BadGateway)
          }
      }
    } ~
    path("thumbnail") {
      fileUpload("thumbnail") {
        case (fileInfo, fileStream: Source[ByteString, Any]) =>
          println(fileInfo)
          val objectName = s"$companyName/$contentName/${fileInfo.fieldName}"
          println(objectName)
          val path: Path = Paths.get(s"${config.rootPath}/$companyName/$contentName/${fileInfo.fileName}")

          upload(ossClient, path, fileStream, objectName, config.bucketName) match {
            case Success(value) =>
              println("x" * 100)
              println(value)
              complete(value)
            case Failure(exception) =>
              println(exception)
              complete(StatusCodes.BadGateway)
          }
      }
    }
  }

  def printRequestMethod(req: HttpRequest): Unit = println(req)
  val logRequestPrintln = DebuggingDirectives.logRequest(LoggingMagnet(_ => printRequestMethod))

  def route(
    ossClient: OSS,
    config: OSSConfig
  )(implicit system: ActorSystem[Nothing], ec: ExecutionContext, materializer: ActorMaterializer): Route = Route.seal {
    pathPrefix("api") {
//      logRequestPrintln(complete("logged"))
      pathPrefix("upload") {
        pathPrefix(Segment) { companyName =>
          authenticateBasic(realm = "secure site", myUserPassAuthenticator(_, companyName)) { userName =>
            println(userName)
            pathPrefix("content") {
              pathPrefix(Segment) { contentName =>
                println(contentName)
                deleteContentRoute(ossClient, config, companyName, contentName) ~ contentJsonRoute(ossClient, config, companyName, contentName) ~ contentUploadRoute(ossClient, config, companyName, contentName)
              }
            } ~ companyJsonRoute(ossClient, config, companyName) ~ companyUploadRoute(ossClient, config, companyName)

          }
        }
      }
    }
  }
}