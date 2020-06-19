package com.example.saga.http
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.http.scaladsl.server.Directives.{as, complete, entity, pathPrefix, _}
import akka.http.scaladsl.server.Route
import akka.util.Timeout
import com.datastax.driver.core.Session
import com.example.graph.GraphNodeEntity.{GraphNodeCommand, GraphNodeCommandReply, To, UpdateEdgeCommand}
import com.example.saga.GetUserBookmarkedByActor.GetUserBookmarkedByReply
import com.example.saga.GetUserBookmarksActor.GetUserBookmarksReply
import com.example.saga.HomePageRecommendationActor.HomePageRecommendationReply
import com.example.saga.Main.{BookmarkNodeCmd, BookmarkUserCmd, GetUserBookmarkedByCmd, GetUserBookmarksCmd, HomePageRecoCmd, NodeRecoCmd, QueryCommand, TrendingNodesCmd}
import com.example.saga.{NodeBookmarkActor, NodeRecommendationActor}
import com.example.saga.NodeBookmarkActor.NodeBookmarkReply
import com.example.saga.NodeRecommendationActor.NodeRecommendationReply
import com.example.saga.NodeTrendingActor.NodeTrendingReply
import com.example.saga.UserBookmarkActor.UserBookmarkReply
import com.example.saga.http.Requests._
import com.example.user.UserNodeEntity
import com.example.user.UserNodeEntity._
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.json4s.{DefaultFormats, Formats, native}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import com.example.saga.GetAllUserBookmarksActor.GetAllUserBookmarksReply
import com.example.saga.Main.GetAllUserBookmarksCmd

object RequestApi extends Json4sSupport {
  implicit val timeout: Timeout = 40.seconds
  implicit val serialization = native.Serialization
  implicit val formats: Formats = DefaultFormats

  def route(
    graphCordinator: ActorRef[ShardingEnvelope[GraphNodeCommand[GraphNodeCommandReply]]],
    userCordinator: ActorRef[ShardingEnvelope[UserCommand[UserReply]]]
  )(implicit system: ActorSystem[QueryCommand], ec: ExecutionContext, session: Session): Route = {
    pathPrefix("api") {
      pathPrefix("home") {
        post {
          entity(as[HomePageVisitReq]) { referralReq: HomePageVisitReq =>
            complete(
              system.ask[HomePageRecommendationReply] { ref =>
                HomePageRecoCmd(referralReq.userId, referralReq.userLabels, ref)
              }
            )
          }
        }
      } ~
      pathPrefix("request") {
        post {
          entity(as[NodeReferralReq]) { referralReq: NodeReferralReq =>
            complete(
              system.ask[NodeRecommendationReply] { ref =>
                NodeRecoCmd(referralReq.nodeId, referralReq.userId, referralReq.userLabels, UserNodeEntity.NodeReferralBias(), ref)
              }
            )
          }
        }
      } ~
      pathPrefix("search") {
        post {
          entity(as[NodeReferralReq]) { referralReq: NodeReferralReq =>
            complete(
              system.ask[NodeRecommendationReply] { ref =>
                NodeRecoCmd(referralReq.nodeId, referralReq.userId, referralReq.userLabels, UserNodeEntity.NodeSearchBias(), ref)
              }
            )
          }
        }
      } ~
      pathPrefix("record") {
        post {
          entity(as[NodeVisitReq]) { updateEdgeReq =>
            complete(
              graphCordinator.ask[GraphNodeCommandReply] { ref =>
                ShardingEnvelope(updateEdgeReq.nodeId, UpdateEdgeCommand(updateEdgeReq.nodeId, NodeRecommendationActor.SagaEdgeType, To(updateEdgeReq.targetNodeId), updateEdgeReq.properties, updateEdgeReq.userId, Some(updateEdgeReq.userLabels), None, ref))
              }
            )
          }
        }
      } ~
      pathPrefix("trending") {
        get {
          complete(
            system.ask[NodeTrendingReply] { ref =>
              TrendingNodesCmd(ref)
            }
          )
        }
      } ~
      pathPrefix("bookmarkedBy") {
        pathPrefix(Segment) { userId =>
          get {
            complete(
              system.ask[GetUserBookmarkedByReply] { ref: ActorRef[GetUserBookmarkedByReply] =>
                GetUserBookmarkedByCmd(userId, ref)
              }
            )
          }
        }
      } ~
      pathPrefix("bookmarked") {
        pathPrefix(Segment) { userId =>
          get {
            complete(
              system.ask[GetAllUserBookmarksReply] { ref: ActorRef[GetAllUserBookmarksReply] =>
                GetAllUserBookmarksCmd(userId, ref)
              }
            )
          }
        }
      } ~
      pathPrefix("bookmark") {
        pathPrefix("user") {
          pathPrefix(Segment) { userId =>
            get {
              complete(
                system.ask[GetUserBookmarksReply] { ref: ActorRef[GetUserBookmarksReply] =>
                  GetUserBookmarksCmd(userId, ref)
                }
              )
            } ~
            delete {
              entity(as[RemoveUserBookmarkReq]) { req: RemoveUserBookmarkReq =>
                val removeBookmarkReq: Future[UserReply] = for {
                  removeBookmark <- userCordinator.ask[UserReply] { ref: ActorRef[UserReply] =>
                    ShardingEnvelope(req.userId, RemoveUserBookmarkRequest(req.userId, req.targetUserId, ref))
                  }
                  _ <- userCordinator.ask[UserReply] { ref: ActorRef[UserReply] =>
                    ShardingEnvelope(req.targetUserId, RemoveBookmarkedByRequest(req.targetUserId, req.userId, ref))
                  }
                } yield {
                  removeBookmark
                }
                complete(removeBookmarkReq)
              }
            }
          } ~
          post {
            entity(as[UserBookmarkReq]) { req =>
              complete(
                system.ask[UserBookmarkReply] { ref: ActorRef[UserBookmarkReply] =>
                  BookmarkUserCmd(req.userId, req.targetUserId, ref)
                }
              )
            }
          }
        } ~
        pathPrefix("node") {
          post {
            entity(as[NodeBookmarkReq]) { req =>
              complete(
                system.ask[NodeBookmarkReply] { ref: ActorRef[NodeBookmarkReply] =>
                  BookmarkNodeCmd(req.nodeId, req.userId, ref)
                }
              )
            }
          } ~
          delete {
            entity(as[RemoveNodeBookmarkReq]) { req: RemoveNodeBookmarkReq =>
              complete(
                userCordinator.ask[UserReply] { ref: ActorRef[UserReply] =>
                  ShardingEnvelope(req.userId, RemoveNodeBookmarkRequest(req.userId, req.nodeId, ref))
                }
              )
            }
          }
        }
      }
    }
  }
}