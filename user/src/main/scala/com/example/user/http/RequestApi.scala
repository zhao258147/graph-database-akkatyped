package com.example.user.http

import akka.actor.typed.{ActorRef, ActorSystem}
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
import akka.http.scaladsl.model.StatusCodes
import com.example.user.Main.{UserGraphRequest, UserMainCommand}
import com.example.user.query.UserGraphQuery
import com.example.user.query.UserGraphQuery.UserGraphQueryReply

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object RequestApi extends Json4sSupport {
  implicit val timeout: Timeout = 40.seconds
  implicit val serialization = native.Serialization
  implicit val formats: Formats = DefaultFormats

  def route(
    userCordinator: ActorRef[ShardingEnvelope[UserCommand[UserReply]]],
  )(implicit system: ActorSystem[UserMainCommand], ec: ExecutionContext, session: Session): Route = {
    pathPrefix("api") {
      pathPrefix("query") {
        post {
          entity(as[NodeVisitReq]) { req =>
            complete(
              userCordinator.ask[UserReply] { ref: ActorRef[UserReply] =>
                ShardingEnvelope(
                  req.userId,
                  NodeVisitRequest(req.userId, req.nodeId, req.tags, req.similarUsers, NodeReferralBias(), ref)
                )
              }
            )
          }
        } ~
        get {
          complete(
            system.ask[UserGraphQueryReply] { req =>
              UserGraphRequest(UserGraphQuery.names, req)
            }
          )
        }
      } ~
      pathPrefix("user") {
        pathPrefix(Segment) { userId =>
          pathPrefix("bookmark") {
            pathPrefix(Segment) { targetUserId =>
              delete {
                complete(
                  userCordinator.ask[UserReply] { ref: ActorRef[UserReply] =>
                    ShardingEnvelope(
                      userId,
                      RemoveBookmarkedByRequest(userId, targetUserId, ref)
                    )
                  }
                )
              }
            }
          } ~
          pathPrefix("autoreply") {
            post {
              entity(as[UpdateAutoReplyReq]) { req =>
                complete(
                  userCordinator.ask[UserReply] { ref: ActorRef[UserReply] =>
                    ShardingEnvelope(
                      userId,
                      SetAutoReplyCommand(userId, req.autoReply, ref)
                    )
                  }
                )
              }
            }
          } ~
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
          put {
            entity(as[UpdateUserReq]) { req =>
              complete(
                userCordinator.ask[UserReply] { ref: ActorRef[UserReply] =>
                  if(req.labels.isEmpty)
                    ShardingEnvelope(
                      req.userId,
                      UpdateUserPropertiesCommand(req.userId, req.properties, ref)
                    )
                  else if(req.properties.isEmpty)
                    ShardingEnvelope(
                      req.userId,
                      UpdateUserLabelsCommand(req.userId, req.labels, ref)
                    )
                  else
                    ShardingEnvelope(
                      req.userId,
                      UpdateUserCommand(req.userId, req.properties, req.labels, ref)
                    )
                }
              )
            }
          }
        } ~
        post {
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
      } ~
      pathPrefix("users") {
        post {
          entity(as[ListUsersReq]) { req =>
            complete(
              system.ask[UserGraphQueryReply] { ref =>
                UserGraphRequest(req.users, ref)
              }
            )
          }
        }
      }
    }
  }
}
