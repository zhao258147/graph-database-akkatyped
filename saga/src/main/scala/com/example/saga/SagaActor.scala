package com.example.saga

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.util.Timeout
import com.datastax.driver.core.Session
import com.example.graph.GraphNodeEntity._
import com.example.graph.readside.ClickReadSideActor.{TrendingNodesCommand, TrendingNodesResponse}
import com.example.graph.readside.NodeReadSideActor.{RelatedNodeQuery, RelatedNodeQueryResponse}
import com.example.graph.readside.{ClickReadSideActor, NodeReadSideActor}
import com.example.user.UserNodeEntity._

import scala.concurrent.duration._

object SagaActor {
  val SagaEdgeType = "User"
  sealed trait SagaActorCommand
  case class NodeReferral(nodeId: NodeId, userId: UserId, userLabels: Map[String, Int], replyTo: ActorRef[SagaActorReply]) extends SagaActorCommand
  case class RelevantNodes(nodes: Seq[NodeQueryResult]) extends SagaActorCommand
  case class WrappedNodeEntityResponse(nodeEntityResponse: GraphNodeCommandReply) extends SagaActorCommand
  case class WrappedUserEntityResponse(userEntityResponse: UserReply) extends SagaActorCommand
  case class WrappedClickActorResponse(trendingResponse: TrendingNodesResponse) extends SagaActorCommand
  case class WrappedNodeActorResponse(nodesResponse: RelatedNodeQueryResponse) extends SagaActorCommand

  sealed trait SagaActorReply
  case class NodeReferralReply(userId: UserId, recommended: Seq[String], relevant: Seq[String], popular: Seq[String], overallRanking: Seq[String], rankingByType: Map[String, Seq[String]]) extends SagaActorReply

  case class NodeQueryResult(nodeType: String, nodeId: String, tags: java.util.Map[String, java.lang.Integer], properties: java.util.Map[String, String])

  private def checkAllResps(
    userEntityResponseMapper: ActorRef[UserReply],
    referral: NodeReferral,
    userShardRegion: ActorRef[ShardingEnvelope[UserCommand[UserReply]]],
    recommendedResult: Option[RecommendedResult],
    nodeActorResponse: Option[RelatedNodeQueryResponse],
    trendingNodesResponse: Option[TrendingNodesResponse]
  ) = for {
    entityResp <- recommendedResult
    nodeResp <- nodeActorResponse
    trendingResp <- trendingNodesResponse
  } yield {
    userShardRegion ! ShardingEnvelope(
      referral.userId,
      NodeVisitRequest(
        referral.userId,
        referral.nodeId,
        entityResp.tags,
        entityResp.similarUserEdges.map(_.direction.nodeId),
        entityResp.popularEdges.map(_.direction.nodeId),
        nodeResp.list.map(_.nodeId),
        userEntityResponseMapper
      )
    )
    (entityResp, nodeResp, trendingResp)
  }

  def apply(
    graphShardRegion: ActorRef[ShardingEnvelope[GraphNodeCommand[GraphNodeCommandReply]]],
    userShardRegion: ActorRef[ShardingEnvelope[UserCommand[UserReply]]],
    clickReadSideActor: ActorRef[ClickReadSideActor.ClickStatCommands],
    nodeReadSideActor: ActorRef[NodeReadSideActor.NodeReadSideCommand]
  )(implicit session: Session): Behavior[SagaActorCommand] = Behaviors.setup { cxt =>
    println(cxt.self)
    implicit val system = cxt.system
    implicit val timeout: Timeout = 30.seconds
    implicit val ex = cxt.executionContext

    val nodeEntityResponseMapper: ActorRef[GraphNodeCommandReply] =
      cxt.messageAdapter(rsp => WrappedNodeEntityResponse(rsp))

    val userEntityResponseMapper: ActorRef[UserReply] =
      cxt.messageAdapter(rsp => WrappedUserEntityResponse(rsp))

    val trendingActorResponseMapper: ActorRef[TrendingNodesResponse] =
      cxt.messageAdapter(rsp => WrappedClickActorResponse(rsp))

    val nodeActorResponseMapper: ActorRef[RelatedNodeQueryResponse] =
      cxt.messageAdapter(rsp => WrappedNodeActorResponse(rsp))

    def initial(): Behavior[SagaActorCommand] =
      Behaviors.receiveMessagePartial {
        case referral: NodeReferral =>
          graphShardRegion ! ShardingEnvelope(
            referral.nodeId,
            QueryRecommended(
              nodeId = referral.nodeId,
              visitorId = referral.userId,
              edgeTypes = Set(SagaEdgeType),
              visitorLabels = referral.userLabels,
              replyTo = nodeEntityResponseMapper
            )
          )

          clickReadSideActor ! TrendingNodesCommand(referral.userLabels.keySet, trendingActorResponseMapper)
          nodeReadSideActor ! RelatedNodeQuery(referral.userLabels, nodeActorResponseMapper)
          waitingForRecommendationReply(referral, None, None, None)
      }

    def waitingForRecommendationReply(referral: NodeReferral, recommendedResult: Option[RecommendedResult], nodeActorResponse: Option[RelatedNodeQueryResponse], trendingNodesResponse: Option[TrendingNodesResponse]): Behavior[SagaActorCommand] =
      Behaviors.receiveMessagePartial {
        case WrappedNodeEntityResponse(wrappedNodeReply: RecommendedResult) =>
          checkAllResps(userEntityResponseMapper, referral, userShardRegion, Some(wrappedNodeReply), nodeActorResponse, trendingNodesResponse)
              .map{
                case (entityResp, nodeResp, trendingResp) =>
                  waitingForUserReply(referral, entityResp, nodeResp, trendingResp)
              }.getOrElse(waitingForRecommendationReply(referral, Some(wrappedNodeReply), nodeActorResponse, trendingNodesResponse))

        case WrappedClickActorResponse(wrappedTrendingReply) =>
          checkAllResps(userEntityResponseMapper, referral, userShardRegion, recommendedResult, nodeActorResponse, Some(wrappedTrendingReply))
            .map{
              case (entityResp, nodeResp, trendingResp) =>
                waitingForUserReply(referral, entityResp, nodeResp, trendingResp)
            }.getOrElse(waitingForRecommendationReply(referral, recommendedResult, nodeActorResponse, Some(wrappedTrendingReply)))

        case WrappedNodeActorResponse(wrappedNodeReply) =>
          checkAllResps(userEntityResponseMapper, referral, userShardRegion, recommendedResult, Some(wrappedNodeReply), trendingNodesResponse)
            .map{
              case (entityResp, nodeResp, trendingResp) =>
                waitingForUserReply(referral, entityResp, nodeResp, trendingResp)
            }.getOrElse(waitingForRecommendationReply(referral, recommendedResult, Some(wrappedNodeReply), trendingNodesResponse))
      }

    def waitingForUserReply(referral: NodeReferral, nodeReply: RecommendedResult, nodeActorResponse: RelatedNodeQueryResponse, trendingNodesResponse: TrendingNodesResponse): Behavior[SagaActorCommand] =
      Behaviors.receiveMessagePartial {
        case WrappedUserEntityResponse(wrapperUserReply: UserRequestSuccess) =>
          println(wrapperUserReply)
          referral.replyTo ! NodeReferralReply(wrapperUserReply.userId, wrapperUserReply.recommended, wrapperUserReply.relevant, wrapperUserReply.popular, trendingNodesResponse.overallRanking, trendingNodesResponse.rankingByType)
          Behaviors.stopped
      }

    initial()
  }


}
