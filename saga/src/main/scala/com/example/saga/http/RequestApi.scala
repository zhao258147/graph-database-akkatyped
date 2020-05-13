package com.example.saga.http
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.http.scaladsl.server.Directives.{as, complete, entity, pathPrefix, _}
import akka.http.scaladsl.server.Route
import akka.util.Timeout
import com.datastax.driver.core.Session
import com.example.graph.GraphNodeEntity.{GraphNodeCommand, GraphNodeCommandReply, To, UpdateEdgeCommand}
import com.example.saga.HomePageRecommendationActor.HomePageRecommendationReply
import com.example.saga.Main.{BookmarkNodeCommand, BookmarkUserCommand, HomePageRecoCommand, NodeRecoCommand, SagaCommand}
import com.example.saga.{NodeBookmarkActor, NodeRecommendationActor}
import com.example.saga.NodeBookmarkActor.NodeBookmarkReply
import com.example.saga.NodeRecommendationActor.NodeRecommendationReply
import com.example.saga.UserBookmarkActor.UserBookmarkReply
import com.example.saga.http.Requests._
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.json4s.{DefaultFormats, Formats, native}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object RequestApi extends Json4sSupport {
  implicit val timeout: Timeout = 40.seconds
  implicit val serialization = native.Serialization
  implicit val formats: Formats = DefaultFormats

  def route(
    graphCordinator: ActorRef[ShardingEnvelope[GraphNodeCommand[GraphNodeCommandReply]]]
  )(implicit system: ActorSystem[SagaCommand], ec: ExecutionContext, session: Session): Route = {
    pathPrefix("api") {
      pathPrefix("home") {
        post {
          entity(as[HomePageVisitReq]) { referralReq: HomePageVisitReq =>
            complete(
              system.ask[HomePageRecommendationReply] { ref =>
                HomePageRecoCommand(referralReq.userId, referralReq.userLabels, ref)
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
                println(referralReq.userLabels)
                NodeRecoCommand(referralReq.nodeId, referralReq.userId, referralReq.userLabels, ref)
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
      pathPrefix("bookmark") {
        pathPrefix("user") {
          post {
            entity(as[UserBookmarkReq]) { req =>
              complete(
                system.ask[UserBookmarkReply] { ref: ActorRef[UserBookmarkReply] =>
                  BookmarkUserCommand(req.userId, req.targetUserId, ref)
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
                  BookmarkNodeCommand(req.nodeId, req.userId, ref)
                }
              )
            }
          }
        }
      }
    }
  }
}