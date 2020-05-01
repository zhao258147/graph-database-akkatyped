package com.example.user.http

import akka.actor.typed.ActorRef
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.http.scaladsl.server.Directives.{as, complete, entity, pathPrefix, put}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.Directives._
import akka.util.Timeout
import com.datastax.driver.core.Session
import com.example.user.UserNodeEntity._
import com.example.user.http.Requests._
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.json4s.{DefaultFormats, Formats, native}
import akka.actor.typed.scaladsl.AskPattern._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object RequestApi extends Json4sSupport {
  implicit val timeout: Timeout = 40.seconds
  implicit val serialization = native.Serialization
  implicit val formats: Formats = DefaultFormats

  def route(
    userCordinator: ActorRef[ShardingEnvelope[UserCommand[UserReply]]],
  )(implicit system: akka.actor.typed.ActorSystem[Nothing], ec: ExecutionContext, session: Session): Route = {
    pathPrefix("api") {
      pathPrefix("query") {
        post {
          entity(as[NodeVisitReq]) { req =>
            complete(
              userCordinator.ask[UserReply] { ref: ActorRef[UserReply] =>
                ShardingEnvelope(
                  req.userId,
                  NodeVisitRequest(req.userId, req.nodeId, req.tags, req.recommended, req.relevant, req.popular, ref)
                )
              }
            )
          }
        }
      } ~
      pathPrefix("user") {
        pathPrefix(Segment) { userId =>
          get {
            complete(
              userCordinator.ask[UserReply] { ref: ActorRef[UserReply] =>
                ShardingEnvelope(
                  userId,
                  UserRetrievalCommand(userId, ref)
                )
              }
            )
          } ~
          post {
            entity(as[UpdateUserReq]) { req =>
              complete(
                userCordinator.ask[UserReply] { ref: ActorRef[UserReply] =>
                  ShardingEnvelope(
                    req.userId,
                    UpdateUserCommand(req.userId, req.userType, req.properties, req.labels, ref)
                  )
                }
              )
            }
          }
        } ~
        put {
          entity(as[CreateUserReq]) { createCommand =>
            complete(
              userCordinator.ask[UserReply] { ref: ActorRef[UserReply] =>
                ShardingEnvelope(
                  createCommand.userId,
                  CreateUserCommand(createCommand.userId, createCommand.userType, createCommand.properties, createCommand.labels, ref)
                )
              }
            )
          }
        }
      }
    }
  }
}
