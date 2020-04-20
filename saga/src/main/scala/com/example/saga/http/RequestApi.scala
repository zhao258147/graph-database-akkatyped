package com.example.saga.http
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.http.scaladsl.server.Directives.{as, complete, entity, pathPrefix, _}
import akka.http.scaladsl.server.Route
import akka.util.Timeout
import com.datastax.driver.core.Session
import com.example.graph.GraphNodeEntity.{GraphNodeCommand, GraphNodeCommandReply, To, UpdateEdgeCommand}
import com.example.saga.Main.{NodeReferralCommand, SagaCommand}
import com.example.saga.SagaActor
import com.example.saga.SagaActor.SagaActorReply
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
      pathPrefix("request") {
        post {
          entity(as[NodeReferralReq]) { referralReq: NodeReferralReq =>
            complete(
              system.ask[SagaActorReply] { ref =>
                NodeReferralCommand(referralReq.nodeId, referralReq.userId, referralReq.userLabels, ref)
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
                ShardingEnvelope(updateEdgeReq.nodeId, UpdateEdgeCommand(updateEdgeReq.nodeId, SagaActor.SagaEdgeType, To(updateEdgeReq.targetNodeId), updateEdgeReq.properties, updateEdgeReq.userId, Some(updateEdgeReq.userLabels), ref))
              }
            )
          }
        }
      }
    }
  }
}