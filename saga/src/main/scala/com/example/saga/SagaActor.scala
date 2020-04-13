package com.example.saga

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.util.Timeout
import com.example.graph.GraphNodeEntity.{EdgeQuery, EdgeQueryResult, GraphNodeCommand, GraphNodeCommandReply, NodeId, TargetNodeId}
import com.example.user.UserNodeEntity._

import scala.concurrent.duration._

object SagaActor {
  sealed trait SagaActorCommand
  case class NodeReferral(nodeId: NodeId, targetNodeId: TargetNodeId, userId: UserId, userLabels: Set[String], replyTo: ActorRef[SagaActorReply]) extends SagaActorCommand
  case class WrappedNodeEntityResponse(nodeEntityResponse: GraphNodeCommandReply) extends SagaActorCommand
  case class WrappedUserEntityResponse(userEntityResponse: UserReply) extends SagaActorCommand

  sealed trait SagaActorReply
  case class NodeReferralReply(userId: UserId, recommended: Seq[String]) extends SagaActorReply

  def apply(
    graphShardRegion: ActorRef[ShardingEnvelope[GraphNodeCommand[GraphNodeCommandReply]]],
    userShardRegion: ActorRef[ShardingEnvelope[UserCommand[UserReply]]]
  ): Behavior[SagaActorCommand] = Behaviors.setup { cxt =>
    implicit val system = cxt.system
    implicit val timeout: Timeout = 30.seconds
    implicit val ex = cxt.executionContext

    val nodeEntityResponseMapper: ActorRef[GraphNodeCommandReply] =
      cxt.messageAdapter(rsp => WrappedNodeEntityResponse(rsp))

    val userEntityResponseMapper: ActorRef[UserReply] =
      cxt.messageAdapter(rsp => WrappedUserEntityResponse(rsp))

    def initial(): Behavior[SagaActorCommand] =
      Behaviors.receiveMessagePartial {
        case referral: NodeReferral =>
          graphShardRegion ! ShardingEnvelope(
            referral.nodeId,
            EdgeQuery(
              referral.nodeId,
              None,
              Map.empty,
              referral.userLabels,
              Map.empty,
              nodeEntityResponseMapper
            )
          )
          waitingForNodeReply(referral)
      }

    def waitingForNodeReply(referral: NodeReferral): Behavior[SagaActorCommand] =
      Behaviors.receiveMessagePartial {
        case WrappedNodeEntityResponse(wrappedNodeReply: EdgeQueryResult) =>
          userShardRegion ! ShardingEnvelope(
            referral.nodeId,
            NodeVisitRequest(
              referral.userId,
              referral.nodeId,
              wrappedNodeReply.tags,
              wrappedNodeReply.edgeResult.map(_.direction.nodeId).toSeq,
              userEntityResponseMapper
            )
          )

          waitingForUserReply(referral, wrappedNodeReply)
      }

    def waitingForUserReply(referral: NodeReferral, nodeReply: GraphNodeCommandReply): Behavior[SagaActorCommand] =
      Behaviors.receiveMessagePartial {
        case WrappedUserEntityResponse(wrapperUserReply: UserRequestSuccess) =>
          referral.replyTo ! NodeReferralReply(wrapperUserReply.userId, wrapperUserReply.recommended)
          Behaviors.stopped
      }

    initial()
  }


}
