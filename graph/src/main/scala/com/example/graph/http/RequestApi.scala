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
import com.example.graph.query.GraphActorSupervisor
import com.example.graph.query.GraphActorSupervisor.{StartEdgeSagaActor, StartGraphQueryActor, StartLocationQueryActor}
import com.example.graph.query.LocationQueryActor.LocationQueryReply
import com.example.graph.saga.EdgeCreationSaga.EdgeCreationReply
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.json4s.{DefaultFormats, Formats, native}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object RequestApi extends Json4sSupport {
  implicit val timeout: Timeout = 40.seconds
  implicit val serialization = native.Serialization
  implicit val formats: Formats = DefaultFormats

  def route(
    graphCordinator: ActorRef[ShardingEnvelope[GraphNodeCommand]],
    graphActorSupervisor: ActorRef[GraphActorSupervisor.GraphQuerySupervisorCommand]
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
                graphActorSupervisor.ask[GraphQueryReply] { ref =>
                  StartGraphQueryActor(query.queries, ref)
                }
              )
            }
          }
        } ~
        pathPrefix("location") {
          get {
            complete(
              graphActorSupervisor.ask[LocationQueryReply] { ref =>
                StartLocationQueryActor(ref)
              })
            }
        } ~
        pathPrefix(Segment) { nodeId =>
          pathPrefix("edge") {
            get {
              parameterMap { params =>
                val edgeTypeParam = "edgeType"
                val nodeTypeParam = "nodeType"

                params.get(edgeTypeParam) match {
                  case Some(edgeType) =>
                    complete(
                      graphCordinator.ask[GraphNodeCommandReply] { ref: ActorRef[GraphNodeCommandReply] =>
                        ShardingEnvelope(nodeId, EdgeQuery(nodeId, params.get(nodeTypeParam), Map.empty, Some(edgeType), params - edgeTypeParam - nodeTypeParam, ref))
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
                    ShardingEnvelope(nodeId, RemoveEdgeCommand(nodeId, removeEdgeReq.targetNodeId, removeEdgeReq.edgeType, ref))
                  }
                )
              }
            } ~
            post {
              entity(as[UpdateEdgeReq]) { updateEdgeReq =>
                updateEdgeReq.direction.toLowerCase match {
                  case "to" =>
                    complete(
                      graphCordinator.ask[GraphNodeCommandReply] { ref =>
                        ShardingEnvelope(nodeId, UpdateEdgeCommand(updateEdgeReq.targetNodeId, updateEdgeReq.edgeType, To(updateEdgeReq.targetNodeId), updateEdgeReq.properties, ref))
                      }
                    )
                  case "from" =>
                    complete(
                      graphCordinator.ask[GraphNodeCommandReply] { ref =>
                        ShardingEnvelope(nodeId, UpdateEdgeCommand(updateEdgeReq.targetNodeId, updateEdgeReq.edgeType, From(updateEdgeReq.targetNodeId), updateEdgeReq.properties, ref))
                      }
                    )
                  case _ =>
                    complete(
                      graphActorSupervisor.ask[EdgeCreationReply] { ref =>
                        StartEdgeSagaActor(nodeId, updateEdgeReq.targetNodeId, updateEdgeReq.edgeType, updateEdgeReq.properties, ref)
                      }
                    )
                }
              }
            }
          } ~
          get {
            complete(
              graphCordinator.ask[GraphNodeCommandReply] { ref =>
                ShardingEnvelope(nodeId, NodeQuery(nodeId, ref))
              }
            )
          } ~
          post {
            entity(as[UpdateNodeReq]) { update =>
              println(update)
              complete(
                graphCordinator.ask[GraphNodeCommandReply] { ref =>
                  ShardingEnvelope(nodeId, UpdateNodeCommand(nodeId, update.properties, ref))
                }
              )
            }
          }
        }
      }
    }
  }
}
