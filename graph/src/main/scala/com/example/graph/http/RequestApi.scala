package com.example.graph.http

import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.AskPattern._
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.util.Timeout
import com.datastax.driver.core.Session
import com.example.graph.GraphNodeEntity._
import com.example.graph.http.Requests._
import com.example.graph.query.GraphQueryActor.GraphQueryReply
import com.example.graph.query.GraphQuerySupervisor
import com.example.graph.query.GraphQuerySupervisor.StartGraphQueryActor
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.json4s.{DefaultFormats, Formats, native}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object RequestApi extends Json4sSupport {
  implicit val timeout: Timeout = 40.seconds
  implicit val serialization = native.Serialization
  implicit val formats: Formats = DefaultFormats

  def route(
    graphCordinator: ActorRef[ShardingEnvelope[GraphNodeCommand[GraphNodeCommandReply]]],
    graphQuerySupervisor: ActorRef[GraphQuerySupervisor.GraphQuerySupervisorCommand]
  )(implicit system: akka.actor.typed.ActorSystem[Nothing], ec: ExecutionContext, session: Session): Route = {

    pathPrefix("api") {
      pathPrefix("graph") {
        put {
          entity(as[CreateNodeReq]) { createCommand =>
            complete(
              graphCordinator.ask[GraphNodeCommandReply] { ref: ActorRef[GraphNodeCommandReply] =>
                ShardingEnvelope(createCommand.nodeId, CreateNodeCommand(createCommand.nodeId, createCommand.nodeType, createCommand.properties, ref))
              }
            )
          }
        } ~
        pathPrefix("query") {
          post {
            entity(as[GraphQueryReq]) { query =>
              complete(
                graphQuerySupervisor.ask[GraphQueryReply] { ref =>
                  StartGraphQueryActor(query.nodeType, query.queries, ref)
                }
              )
            }
          }
        } ~
        pathPrefix(Segment) { nodeId =>
          get {
            complete(
              graphCordinator.ask[GraphNodeCommandReply] { ref =>
                ShardingEnvelope(nodeId, NodeQuery(ref))
              }
            )
          } ~
          post {
            entity(as[UpdateNodeReq]) { update =>
              complete(
                graphCordinator.ask[GraphNodeCommandReply] { ref =>
                  ShardingEnvelope(nodeId, UpdateNodeCommand(update.properties, ref))
                }
              )
            }
          } ~
          pathPrefix("edge") {
            get {
              parameterMap { params =>
                val edgeTypeParam = "edgeType"
                params.get(edgeTypeParam) match {
                  case Some(edgeType) =>
                    complete(
                      graphCordinator.ask[GraphNodeCommandReply] { ref: ActorRef[GraphNodeCommandReply] =>
                        ShardingEnvelope(nodeId, EdgeQuery(None, Map.empty, Some(edgeType), params - edgeTypeParam, ref))
                      }
                    )
                  case None =>
                    complete("no edge type defined")
                }
              }
            } ~
            delete {
              entity(as[RemoveEdgeReq]) { removeEdgeReq =>
                complete(
                  graphCordinator.ask[GraphNodeCommandReply] { ref =>
                    ShardingEnvelope(nodeId, RemoveEdge(removeEdgeReq.targetNodeId, removeEdgeReq.edgeType, ref))
                  }
                )
              }
            } ~
            post {
              entity(as[UpdateEdgeReq]) { updateEdgeReq =>
                println(updateEdgeReq)
                val direction = updateEdgeReq.direction match {
                  case _ =>
                    To(updateEdgeReq.targetNodeId)
                }
                complete(
                  graphCordinator.ask[GraphNodeCommandReply] { ref =>
                    ShardingEnvelope(nodeId, UpdateEdge(updateEdgeReq.targetNodeId, updateEdgeReq.edgeType, direction, updateEdgeReq.properties, ref))
                  }
                )
              }
            }
          }
        }
      }
    }
  }
}
