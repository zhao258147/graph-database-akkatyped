package com.example.saga

import akka.actor.typed.scaladsl.{Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.sharding.typed.ShardingEnvelope
import com.datastax.driver.core.Session
import com.example.graph.GraphNodeEntity._
import com.example.graph.readside.ClickReadSideActor.{TrendingNodesCommand, TrendingNodesResponse}
import com.example.graph.readside.{ClickReadSideActor, NodeReadSideActor}
import com.example.saga.readside.SagaNodeReadSideActor._
import com.example.user.UserNodeEntity._

import scala.concurrent.duration._

object NodeRecommendationActor {
  val SagaEdgeType = "User"

  val RecoTimeoutMessage = "Could not produce recommendations in time"
  val RecoUnknownError = "Unknown error"
  val RecommendationSagaTimeoutMS = 4000 millis

  case class UserDisplayInfo(userId: UserId, userType: String, properties: Map[String, String], labels: Map[String, Int])

  sealed trait NodeRecommendationCommand
  case object NodeRecommendationTimeout extends NodeRecommendationCommand
  case class NodeReferral(nodeId: NodeId, userId: UserId, userLabels: Map[String, Int], replyTo: ActorRef[NodeRecommendationReply]) extends NodeRecommendationCommand
  case class WrappedNodeEntityResponse(nodeEntityResponse: GraphNodeCommandReply) extends NodeRecommendationCommand
  case class WrappedUserEntityResponse(userEntityResponse: UserReply) extends NodeRecommendationCommand
  case class WrappedClickActorResponse(trendingResponse: TrendingNodesResponse) extends NodeRecommendationCommand
  case class WrappedSagaNodeActorResponse(nodesResponse: SagaNodeReadSideResponse) extends NodeRecommendationCommand

  sealed trait NodeRecommendationReply
  case class NodeRecommendationSuccess(userId: String, nodeId: String, nodeType: String, tags: Map[String, Int], nodeProperties: Map[String, String], updatedLabels: Map[String, Int], recommended: Seq[RecoWithNodeInfo], relevant: Seq[RecoWithNodeInfo], popular: Seq[RecoWithNodeInfo], overallRanking: Seq[RecoWithNodeInfo], rankingByType: Map[String, Seq[RecoWithNodeInfo]], neighbourHistory: Seq[RecoWithNodeInfo], similarUsers: Seq[UserDisplayInfo]) extends NodeRecommendationReply
  case class NodeRecommendationFailed(message: String) extends NodeRecommendationReply

  private def checkAllResps(
    userEntityResponseMapper: ActorRef[UserReply],
    referral: NodeReferral,
    userShardRegion: ActorRef[ShardingEnvelope[UserCommand[UserReply]]],
    recommendedResult: Option[RecommendedResult],
    nodeActorResponse: Option[SagaNodesQueryResponse],
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
        entityResp.similarUsers.map(s => s.visitorId -> s.labels).toMap,
        userEntityResponseMapper
      )
    )
    (entityResp, nodeResp, trendingResp)
  }

  def apply(
    graphShardRegion: ActorRef[ShardingEnvelope[GraphNodeCommand[GraphNodeCommandReply]]],
    userShardRegion: ActorRef[ShardingEnvelope[UserCommand[UserReply]]],
    clickReadSideActor: ActorRef[ClickReadSideActor.ClickStatCommands],
    sagaNodeReadSideActor: ActorRef[SagaNodeReadSideCommand]
  )(implicit session: Session): Behavior[NodeRecommendationCommand] =
    Behaviors.withTimers(timers => sagaBehaviour(graphShardRegion, userShardRegion, clickReadSideActor, sagaNodeReadSideActor, timers))

  def sagaBehaviour(
    graphShardRegion: ActorRef[ShardingEnvelope[GraphNodeCommand[GraphNodeCommandReply]]],
    userShardRegion: ActorRef[ShardingEnvelope[UserCommand[UserReply]]],
    clickReadSideActor: ActorRef[ClickReadSideActor.ClickStatCommands],
    sagaNodeReadSideActor: ActorRef[SagaNodeReadSideCommand],
    timer: TimerScheduler[NodeRecommendationCommand]
  )(implicit session: Session): Behavior[NodeRecommendationCommand] = Behaviors.setup { cxt =>
    val nodeEntityResponseMapper: ActorRef[GraphNodeCommandReply] =
      cxt.messageAdapter(rsp => WrappedNodeEntityResponse(rsp))

    val userEntityResponseMapper: ActorRef[UserReply] =
      cxt.messageAdapter(rsp => WrappedUserEntityResponse(rsp))

    val trendingActorResponseMapper: ActorRef[TrendingNodesResponse] =
      cxt.messageAdapter(rsp => WrappedClickActorResponse(rsp))

    val sagaNodeActorResponseMapper: ActorRef[SagaNodeReadSideResponse] =
      cxt.messageAdapter(rsp => WrappedSagaNodeActorResponse(rsp))

    def initial(): Behavior[NodeRecommendationCommand] =
      Behaviors.receiveMessage {
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
          sagaNodeReadSideActor ! RelatedNodeQuery(referral.userLabels, sagaNodeActorResponseMapper)

          timer.startSingleTimer(NodeRecommendationTimeout, RecommendationSagaTimeoutMS)
          waitingForRecommendationReply(referral, None, None, None)

        case x =>
          println(x)
          Behaviors.stopped
      }

    def waitingForRecommendationReply(referral: NodeReferral, recommendedResult: Option[RecommendedResult], nodeActorResponse: Option[SagaNodesQueryResponse], trendingNodesResponse: Option[TrendingNodesResponse]): Behavior[NodeRecommendationCommand] =
      Behaviors.receiveMessage {
        case WrappedNodeEntityResponse(wrappedNodeReply: GraphNodeCommandFailed) =>
          referral.replyTo ! NodeRecommendationFailed(wrappedNodeReply.error)
          Behaviors.stopped

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

        case WrappedSagaNodeActorResponse(wrappedNodeReply: SagaNodesQueryResponse) =>
          checkAllResps(userEntityResponseMapper, referral, userShardRegion, recommendedResult, Some(wrappedNodeReply), trendingNodesResponse)
            .map{
              case (entityResp, nodeResp, trendingResp) =>
                waitingForUserReply(referral, entityResp, nodeResp, trendingResp)
            }.getOrElse(waitingForRecommendationReply(referral, recommendedResult, Some(wrappedNodeReply), trendingNodesResponse))

        case NodeRecommendationTimeout =>
          //TODO: implement this to return any results that came back
          println(s"(recommendedResult, nodeActorResponse, trendingNodesResponse) (${recommendedResult.isDefined}, ${nodeActorResponse.isDefined}, ${trendingNodesResponse.isDefined})")
          referral.replyTo ! NodeRecommendationFailed(RecoTimeoutMessage)
          Behaviors.stopped

        case x =>
          referral.replyTo ! NodeRecommendationFailed(RecoUnknownError)
          Behaviors.stopped
      }

    def waitingForUserReply(referral: NodeReferral, nodeReply: RecommendedResult, nodeActorResponse: SagaNodesQueryResponse, trendingNodesResponse: TrendingNodesResponse): Behavior[NodeRecommendationCommand] =
      Behaviors.receiveMessagePartial {
        case WrappedUserEntityResponse(wrapperUserReply: NodeVisitRequestSuccess) =>
          println(wrapperUserReply)

          if(wrapperUserReply.neighbours.isEmpty) {
            sagaNodeReadSideActor ! RetrieveNodesQuery(nodeReply.tagMatching -- wrapperUserReply.recentViews, nodeReply.edgeWeight -- wrapperUserReply.recentViews, nodeActorResponse.list -- wrapperUserReply.recentViews, trendingNodesResponse.overallRanking, Map.empty, trendingNodesResponse.rankingByType, sagaNodeActorResponseMapper)
            waitingForFinalNeighbourViewsFilter(referral, nodeReply, wrapperUserReply, Seq.empty)
          } else {
            wrapperUserReply.neighbours.foreach{ userId =>
              userShardRegion ! ShardingEnvelope(
                userId,
                NeighbouringViewsRequest(userId, userEntityResponseMapper)
              )
            }

            waitingForNeighbourViewsReply(referral, nodeReply, wrapperUserReply, nodeReply.tagMatching, nodeReply.edgeWeight, nodeActorResponse.list, trendingNodesResponse.overallRanking, trendingNodesResponse.rankingByType, wrapperUserReply.neighbours, Map.empty, wrapperUserReply.recentViews, Seq.empty)
          }

        case WrappedUserEntityResponse(wrapperUserReply: UserCommandFailed) =>
          referral.replyTo ! NodeRecommendationFailed(wrapperUserReply.error)
          Behaviors.stopped

        case NodeRecommendationTimeout =>
          //TODO: implement this to return any results that came back
          referral.replyTo ! NodeRecommendationFailed(RecoTimeoutMessage)
          Behaviors.stopped

        case x =>
          referral.replyTo ! NodeRecommendationFailed(RecoUnknownError)
          Behaviors.stopped
      }

    def waitingForNeighbourViewsReply(referral: NodeReferral, nodeReply: RecommendedResult, userReply: NodeVisitRequestSuccess, tagMatching: Map[String, Int], edgeWeight: Map[String, Int], relevant: Map[String, Int], trending: Map[String, Int], trendingByTag: Map[String, Seq[(String, Int)]], neighbours: Set[UserId], viewsCollected: Map[String, Int], viewed: Seq[String], neighbourUsers: Seq[UserDisplayInfo]): Behavior[NodeRecommendationCommand] =
      Behaviors.receiveMessage {
        case WrappedUserEntityResponse(wrapperUserReply: NeighbouringViews) =>
          val updatedViews: Map[String, Weight] = viewsCollected ++ wrapperUserReply.viewed.map{ nodeId =>
            nodeId -> (viewsCollected.getOrElse(nodeId, 0) + 1)
          }

          val updatedNeighboursInfo = UserDisplayInfo(wrapperUserReply.userId, wrapperUserReply.userType, wrapperUserReply.properties, wrapperUserReply.labels) +: neighbourUsers

          if(neighbours.size == updatedNeighboursInfo.size) {
            sagaNodeReadSideActor ! RetrieveNodesQuery(tagMatching -- viewed, edgeWeight -- viewed, relevant -- viewed, trending, updatedViews -- viewed, trendingByTag, sagaNodeActorResponseMapper)
            waitingForFinalNeighbourViewsFilter(referral, nodeReply, userReply, updatedNeighboursInfo)
          } else waitingForNeighbourViewsReply(referral, nodeReply, userReply, tagMatching, edgeWeight, relevant, trending, trendingByTag, neighbours, viewsCollected, viewed, updatedNeighboursInfo)

        case NodeRecommendationTimeout =>
          //TODO: implement this to return any results that came back
          println(s"(neighbours, updatedNeighboursInfo) (${neighbours}, ${neighbourUsers})")
          referral.replyTo ! NodeRecommendationFailed(RecoTimeoutMessage)
          Behaviors.stopped

        case x =>
          referral.replyTo ! NodeRecommendationFailed(RecoUnknownError)
          Behaviors.stopped
      }

    def waitingForFinalNeighbourViewsFilter(referral: NodeReferral, nodeReply: RecommendedResult, userReply: NodeVisitRequestSuccess, neighbourUsers: Seq[UserDisplayInfo]): Behavior[NodeRecommendationCommand] =
      Behaviors.receiveMessagePartial {
        case WrappedSagaNodeActorResponse(nodesResponse: SagaNodesInfoResponse) =>
          println("WrappedSagaNodeActorResponse")
          println(nodesResponse)
          referral.replyTo ! NodeRecommendationSuccess(referral.userId, referral.nodeId, nodeReply.nodeType, nodeReply.tags, nodeReply.properties, userReply.updatedLabels, nodesResponse.tagMatching, nodesResponse.relevant, nodesResponse.edgeWeight, nodesResponse.trending, nodesResponse.trendingByTag, nodesResponse.neighbourHistory, neighbourUsers)
          Behaviors.stopped

        case NodeRecommendationTimeout =>
          //TODO: implement this to return any results that came back
          referral.replyTo ! NodeRecommendationFailed(RecoTimeoutMessage)
          Behaviors.stopped

        case x =>
          referral.replyTo ! NodeRecommendationFailed(RecoUnknownError)
          Behaviors.stopped
      }

    initial()
  }


}
