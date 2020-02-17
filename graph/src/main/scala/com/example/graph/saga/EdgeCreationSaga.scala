package com.example.graph.saga

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.cluster.sharding.typed.ShardingEnvelope
import com.example.graph.GraphNodeEntity.{EdgeProperties, EdgeType, From, GraphNodeCommand, GraphNodeCommandFailed, GraphNodeCommandReply, GraphNodeCommandSuccess, NodeId, RemoveEdgeCommand, TargetNodeId, To, UpdateEdgeCommand}

import scala.concurrent.ExecutionContextExecutor

object EdgeCreationSaga {
  sealed trait EdgeCreationCommand
  case class EdgeCreation(fromNodeId: NodeId, targetNodeId: TargetNodeId, edgeType: EdgeType, properties: EdgeProperties, replyTo: ActorRef[EdgeCreationReply]) extends EdgeCreationCommand
  case class NodeEntityResponse(resp: GraphNodeCommandReply) extends EdgeCreationCommand

  sealed trait EdgeCreationReply
  case class EdgeCreationReplySuccessful() extends EdgeCreationReply
  case class EdgeCreationReplyFailed() extends EdgeCreationReply

  def EdgeCreationBehaviour(
    graphCordinator: ActorRef[ShardingEnvelope[GraphNodeCommand]]
  ): Behavior[EdgeCreationCommand] = Behaviors.setup[EdgeCreationCommand] { context =>
    implicit val system: ActorSystem[Nothing] = context.system
    implicit val ex: ExecutionContextExecutor = context.executionContext

    val nodeEntityResponseMapper: ActorRef[GraphNodeCommandReply] =
      context.messageAdapter(rsp => NodeEntityResponse(rsp))

    def entityResponse(
      edgeCreationReq: EdgeCreation,
      fromNodeRespReceived: Boolean,
      targetNodeRespReceived: Boolean,
      replyTo: ActorRef[EdgeCreationReply]
    ): Behavior[EdgeCreationCommand] =
      Behaviors.receiveMessagePartial {
        case NodeEntityResponse(GraphNodeCommandSuccess(nodeId, _)) =>
          println("NodeEntityResponse " * 10)
          println(nodeId)
          if(nodeId == edgeCreationReq.fromNodeId) {
            if(targetNodeRespReceived){
              replyTo ! EdgeCreationReplySuccessful()
              Behaviors.stopped
            } else entityResponse(edgeCreationReq, true, false, replyTo)
          } else if (nodeId == edgeCreationReq.targetNodeId) {
            if(fromNodeRespReceived){
              replyTo ! EdgeCreationReplySuccessful()
              Behaviors.stopped
            } else entityResponse(edgeCreationReq, false, true, replyTo)
          } else Behaviors.stopped

        case NodeEntityResponse(GraphNodeCommandFailed(nodeId, _)) =>
          println("GraphNodeCommandFailed " * 10)
          println(nodeId)
          //TODO, compensation action, need to have new edge command, add edges in sequence
//          if(nodeId == edgeCreationReq.fromNodeId) {
//            if(targetNodeRespReceived) {
//              graphCordinator.tell(
//                ShardingEnvelope(
//                  edgeCreationReq.targetNodeId,
//                  RemoveEdgeCommand(
//                    edgeCreationReq.targetNodeId,
//                    edgeCreationReq.fromNodeId,
//                    edgeCreationReq.edgeType,
//                    nodeEntityResponseMapper)
//
//                )
//              )
//            }
//          }
//
//          if(nodeId == edgeCreationReq.targetNodeId) {
//            if(fromNodeRespReceived) {
//              graphCordinator.tell(
//                ShardingEnvelope(
//                  edgeCreationReq.fromNodeId,
//                  RemoveEdgeCommand(
//                    edgeCreationReq.fromNodeId,
//                    edgeCreationReq.targetNodeId,
//                    edgeCreationReq.edgeType,
//                    nodeEntityResponseMapper)
//
//                )
//              )
//            }
//          }
          Behaviors.stopped
      }

    val initial: Behavior[EdgeCreationCommand] =
      Behaviors.receiveMessagePartial {
        case ec: EdgeCreation =>
          println("EdgeCreation received ")
          graphCordinator.tell(
            ShardingEnvelope(
              ec.fromNodeId,
              UpdateEdgeCommand(ec.fromNodeId, ec.edgeType, To(ec.targetNodeId), ec.properties, nodeEntityResponseMapper)
            )
          )
          graphCordinator.tell(
            ShardingEnvelope(
              ec.targetNodeId,
              UpdateEdgeCommand(ec.targetNodeId, ec.edgeType, From(ec.fromNodeId), ec.properties, nodeEntityResponseMapper)
            )
          )
          entityResponse(ec, false, false, ec.replyTo)
      }

    initial
  }

}
