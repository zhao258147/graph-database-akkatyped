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
import com.example.graph.query.GraphActorSupervisor.{GraphQueryProgress, StartEdgeSagaActor, StartGraphQueryActor, StartWeightQuery}
import com.example.graph.query.NodesQueryActor.NodesQueryReply
import com.example.graph.readside.{ClickReadSideActor, NodeReadSideActor}
import com.example.graph.readside.ClickReadSideActor.{TrendingNodesCommand, TrendingNodesResponse}
import com.example.graph.readside.NodeReadSideActor.{RelatedNodeQuery, RelatedNodeQueryResponse}
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
    graphCordinator: ActorRef[ShardingEnvelope[GraphNodeCommand[GraphNodeCommandReply]]],
    graphActorSupervisor: ActorRef[GraphActorSupervisor.GraphQuerySupervisorCommand],
  )(implicit system: akka.actor.typed.ActorSystem[Nothing], ec: ExecutionContext, session: Session): Route = {


    pathPrefix("api") {
      pathPrefix("graph") {
        put {
          entity(as[CreateNodeReq]) { createCommand =>
            complete(
              graphCordinator.ask[GraphNodeCommandReply] { ref: ActorRef[GraphNodeCommandReply] =>
                ShardingEnvelope(createCommand.nodeId, CreateNodeCommand(createCommand.nodeId, createCommand.nodeType, createCommand.companyId, createCommand.tags, createCommand.properties, ref))
              }
            )
          }
        } ~
//        pathPrefix("trending") {
//          post {
//            entity(as[TrendingNodesReq]) { query =>
//              complete(
//                clickReadSideActor.ask[TrendingNodesResponse] { ref =>
//                  TrendingNodesCommand(query.tags, ref)
//                }
//              )
//            }
//          }
//        } ~
//        pathPrefix("relevant") {
//          post {
//            entity(as[RelevantNodesReq]) { query =>
//              complete(
//                nodeReadSideActor.ask[RelatedNodeQueryResponse] { ref =>
//                  RelatedNodeQuery(query.tags, ref)
//                }
//              )
//            }
//          }
//        } ~
        pathPrefix("query") {
          pathPrefix(Segment) { queryId =>
            get {
              complete(
                graphActorSupervisor.ask[GraphQueryReply] { ref =>
                  GraphQueryProgress(queryId, ref)
                }
              )
            }
          } ~
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
        pathPrefix("nodes") {
          post {
            entity(as[NodesQueryReq]) { query =>
              complete(
                graphActorSupervisor.ask[NodesQueryReply] { ref =>
                  StartWeightQuery(query.nodeIds, ref)
                }
              )
            }
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
                        ShardingEnvelope(
                          nodeId,
                          EdgeQuery(
                            nodeId,
                            params.get(nodeTypeParam),
                            Map.empty,
                            Set(edgeType),
                            params - edgeTypeParam - nodeTypeParam,
                            ref
                          )
                        )
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
                        ShardingEnvelope(
                          nodeId,
                          UpdateEdgeCommand(
                            nodeId,
                            updateEdgeReq.edgeType,
                            To(updateEdgeReq.targetNodeId),
                            updateEdgeReq.properties,
                            updateEdgeReq.userId,
                            if(updateEdgeReq.userLabels.isEmpty) None else Some(updateEdgeReq.userLabels),
                            if(updateEdgeReq.weight == 0) None else Some(updateEdgeReq.weight),
                            ref
                          )
                        )
                      }
                    )
                  case "from" =>
                    complete(
                      graphCordinator.ask[GraphNodeCommandReply] { ref =>
                        ShardingEnvelope(
                          nodeId,
                          UpdateEdgeCommand(
                            nodeId,
                            updateEdgeReq.edgeType,
                            From(updateEdgeReq.targetNodeId),
                            updateEdgeReq.properties,
                            updateEdgeReq.userId,
                            if(updateEdgeReq.userLabels.isEmpty) None else Some(updateEdgeReq.userLabels),
                            if(updateEdgeReq.weight == 0) None else Some(updateEdgeReq.weight),
                            ref
                          )
                        )
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
              complete(
                graphCordinator.ask[GraphNodeCommandReply] { ref =>
                  ShardingEnvelope(nodeId, UpdateNodeCommand(nodeId, update.tags, update.properties, ref))
                }
              )
            }
          }
        }
      }
    }
  }
}
