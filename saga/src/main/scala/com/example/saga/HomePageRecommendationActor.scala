package com.example.saga

import akka.actor.typed.scaladsl.{Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.sharding.typed.ShardingEnvelope
import com.datastax.driver.core.Session
import com.example.graph.readside.ClickReadSideActor.{TrendingNodesCommand, TrendingNodesResponse}
import com.example.graph.readside.{ClickReadSideActor, NodeReadSideActor}
import com.example.user.UserNodeEntity._
import NodeRecommendationActor._
import com.example.saga.readside.SagaNodeReadSideActor.{apply => _, _}
import com.example.saga.readside.SagaUserReadSideActor.{RetrieveUsersQuery, SagaUserInfoResponse}
import com.example.saga.readside.{SagaNodeReadSideActor, SagaUserReadSideActor}

object HomePageRecommendationActor {
  sealed trait HomePageRecommendationCommand
  case object HomePageRecommendationTimeout extends HomePageRecommendationCommand
  case class HomePageRecommendation(userId: UserId, userLabels: Map[String, Int], replyTo: ActorRef[HomePageRecommendationReply]) extends HomePageRecommendationCommand
  case class WrappedUserEntityResponse(userEntityResponse: UserReply) extends HomePageRecommendationCommand
  case class WrappedClickActorResponse(trendingResponse: TrendingNodesResponse) extends HomePageRecommendationCommand
  case class WrappedSagaNodeActorResponse(nodesResponse: SagaNodeReadSideResponse) extends HomePageRecommendationCommand
  case class WrappedSagaUserActorResponse(usersResponse: SagaUserInfoResponse) extends HomePageRecommendationCommand

  sealed trait HomePageRecommendationReply
  case class HomePageRecoReply(userId: String, relevant: Seq[RecoWithNodeInfo], overallRanking: Seq[RecoWithNodeInfo], rankingByType: Map[String, Seq[RecoWithNodeInfo]], neighbourHistory: Seq[RecoWithNodeInfo]) extends HomePageRecommendationReply
  case class HomePageRecoFailed(message: String) extends HomePageRecommendationReply

  private def checkAllResps(
    userEntityResponseMapper: ActorRef[UserReply],
    referral: HomePageRecommendation,
    userShardRegion: ActorRef[ShardingEnvelope[UserCommand[UserReply]]],
    userHistoryResponse: Option[UserHistoryResponse],
    nodeActorResponse: Option[SagaNodesQueryResponse],
    trendingNodesResponse: Option[TrendingNodesResponse]
  ): Option[(UserHistoryResponse, SagaNodesQueryResponse, TrendingNodesResponse)] = for {
    nodeResp <- nodeActorResponse
    trendingResp <- trendingNodesResponse
    userResp <- userHistoryResponse
  } yield {
    userResp.neighbours.foreach{ userId =>
      userShardRegion ! ShardingEnvelope(
        userId,
        NeighbouringViewsRequest(userId, userEntityResponseMapper)
      )
    }

    (userResp, nodeResp, trendingResp)
  }

  def apply(
    userShardRegion: ActorRef[ShardingEnvelope[UserCommand[UserReply]]],
    clickReadSideActor: ActorRef[ClickReadSideActor.ClickStatCommands],
    sagaNodeReadSideActor: ActorRef[SagaNodeReadSideActor.SagaNodeReadSideCommand],
    sagaUserReadSideActor: ActorRef[SagaUserReadSideActor.SagaUserReadSideCommand],
  )(implicit session: Session): Behavior[HomePageRecommendationCommand] =
    Behaviors.withTimers(timers => sagaBehaviour(userShardRegion, clickReadSideActor, sagaNodeReadSideActor, sagaUserReadSideActor, timers))

  def sagaBehaviour(
    userShardRegion: ActorRef[ShardingEnvelope[UserCommand[UserReply]]],
    clickReadSideActor: ActorRef[ClickReadSideActor.ClickStatCommands],
    sagaNodeReadSideActor: ActorRef[SagaNodeReadSideActor.SagaNodeReadSideCommand],
    sagaUserReadSideActor: ActorRef[SagaUserReadSideActor.SagaUserReadSideCommand],
    timer: TimerScheduler[HomePageRecommendationCommand]
  )(implicit session: Session): Behavior[HomePageRecommendationCommand] = Behaviors.setup { cxt =>
    val userEntityResponseMapper: ActorRef[UserReply] =
      cxt.messageAdapter(rsp => WrappedUserEntityResponse(rsp))

    val trendingActorResponseMapper: ActorRef[TrendingNodesResponse] =
      cxt.messageAdapter(rsp => WrappedClickActorResponse(rsp))

    val sagaNodeActorResponseMapper: ActorRef[SagaNodeReadSideResponse] =
      cxt.messageAdapter(rsp => WrappedSagaNodeActorResponse(rsp))

    val sagaUserActorResponseMapper: ActorRef[SagaUserInfoResponse] =
      cxt.messageAdapter(rsp => WrappedSagaUserActorResponse(rsp))

    def initial(): Behavior[HomePageRecommendationCommand] =
      Behaviors.receiveMessage {
        case referral: HomePageRecommendation =>
          clickReadSideActor ! TrendingNodesCommand(referral.userLabels.keySet, trendingActorResponseMapper)
          sagaNodeReadSideActor ! RelatedNodeQuery(referral.userLabels, sagaNodeActorResponseMapper)
          userShardRegion ! ShardingEnvelope(
            referral.userId,
            UserHistoryRetrivalRequest(referral.userId, userEntityResponseMapper)
          )

          timer.startSingleTimer(HomePageRecommendationTimeout, RecommendationSagaTimeoutMS)
          waitingForRecommendationReply(referral, None, None, None)

        case x =>
          println(x)
          Behaviors.stopped
      }

    def waitingForRecommendationReply(referral: HomePageRecommendation, userHistoryResponse: Option[UserHistoryResponse], nodeActorResponse: Option[SagaNodesQueryResponse], trendingNodesResponse: Option[TrendingNodesResponse]): Behavior[HomePageRecommendationCommand] =
      Behaviors.receiveMessage {
        case WrappedClickActorResponse(wrappedTrendingReply) =>
          checkAllResps(userEntityResponseMapper, referral, userShardRegion, userHistoryResponse, nodeActorResponse, Some(wrappedTrendingReply))
            .map{
              case (userResp, nodeResp, trendingResp) =>
                waitingForNeighbourViewsReply(referral, nodeResp.list, trendingResp, userResp.neighbours, Map.empty, userResp.viewed)
            }.getOrElse(waitingForRecommendationReply(referral, userHistoryResponse, nodeActorResponse, Some(wrappedTrendingReply)))

        case WrappedSagaNodeActorResponse(wrappedNodeReply: SagaNodesQueryResponse) =>
          checkAllResps(userEntityResponseMapper, referral, userShardRegion, userHistoryResponse, Some(wrappedNodeReply), trendingNodesResponse)
            .map{
              case (userResp, nodeResp, trendingResp) =>
                waitingForNeighbourViewsReply(referral, nodeResp.list, trendingResp, userResp.neighbours, Map.empty, userResp.viewed)
            }.getOrElse(waitingForRecommendationReply(referral, userHistoryResponse, Some(wrappedNodeReply), trendingNodesResponse))

        case WrappedUserEntityResponse(wrapperUserReply: UserHistoryResponse) =>
          checkAllResps(userEntityResponseMapper, referral, userShardRegion, Some(wrapperUserReply), nodeActorResponse, trendingNodesResponse)
            .map{
              case (userResp, nodeResp, trendingResp) =>
                waitingForNeighbourViewsReply(referral, nodeResp.list, trendingResp, userResp.neighbours, Map.empty, userResp.viewed)
            }.getOrElse(waitingForRecommendationReply(referral, Some(wrapperUserReply), nodeActorResponse, trendingNodesResponse))

        case HomePageRecommendationTimeout =>
          //TODO: implement this to return any results that came back
          referral.replyTo ! HomePageRecoFailed(RecoTimeoutMessage)
          Behaviors.stopped

        case x =>
          println(x)
          referral.replyTo ! HomePageRecoFailed(RecoUnknownError)
          Behaviors.stopped
      }

    def waitingForNeighbourViewsReply(referral: HomePageRecommendation, relevantNodes: Map[String, Int], trendingResp: TrendingNodesResponse, neighbours: Set[String], viewsCollected: Map[String, Int], userViewHistory: Seq[String]): Behavior[HomePageRecommendationCommand] =
      Behaviors.receiveMessage {
        case WrappedUserEntityResponse(wrapperUserReply: NeighbouringViews) =>
          val neighboursLeft = neighbours - wrapperUserReply.userId
          val updatedViews = viewsCollected ++ wrapperUserReply.viewed.map{ nodeId =>
            nodeId -> (viewsCollected.getOrElse(nodeId, 0) + 1)
          }

          if(neighboursLeft.isEmpty) {
            sagaNodeReadSideActor ! RetrieveNodesQuery(Map.empty, Map.empty, relevantNodes -- userViewHistory, trendingResp.overallRanking, updatedViews -- userViewHistory, trendingResp.rankingByType, sagaNodeActorResponseMapper)
            sagaUserReadSideActor ! RetrieveUsersQuery(sagaUserActorResponseMapper, neighbours)

            waitingForFinalNeighbourViewsFilter(referral, None, None)
          } else waitingForNeighbourViewsReply(referral, relevantNodes, trendingResp, neighboursLeft, updatedViews, userViewHistory)

        case HomePageRecommendationTimeout =>
          //TODO: implement this to return any results that came back
          referral.replyTo ! HomePageRecoFailed(RecoTimeoutMessage)
          Behaviors.stopped

        case x =>
          println(x)
          referral.replyTo ! HomePageRecoFailed(RecoUnknownError)
          Behaviors.stopped
      }

    def waitingForFinalNeighbourViewsFilter(referral: HomePageRecommendation, sagaNodeOpt: Option[SagaNodesInfoResponse], sagaUserOpt: Option[SagaUserInfoResponse]): Behavior[HomePageRecommendationCommand] =
      Behaviors.receiveMessage {
        case WrappedSagaNodeActorResponse(sagaNodeReply: SagaNodesInfoResponse) =>
          sagaUserOpt match {
            case Some(sagaUserResp) =>
              referral.replyTo ! HomePageRecoReply(referral.userId, sagaNodeReply.relevant, sagaNodeReply.trending, sagaNodeReply.trendingByTag, sagaNodeReply.neighbourHistory)
              Behaviors.stopped
            case None =>
              waitingForFinalNeighbourViewsFilter(referral, Some(sagaNodeReply), sagaUserOpt)
          }

        case WrappedSagaUserActorResponse(sagaUserReply: SagaUserInfoResponse) =>
          sagaNodeOpt match {
            case Some(sagaNodeReply) =>
              referral.replyTo ! HomePageRecoReply(referral.userId, sagaNodeReply.relevant, sagaNodeReply.trending, sagaNodeReply.trendingByTag, sagaNodeReply.neighbourHistory)
              Behaviors.stopped
            case None =>
              waitingForFinalNeighbourViewsFilter(referral, sagaNodeOpt, Some(sagaUserReply))
          }

        case HomePageRecommendationTimeout =>
          //TODO: implement this to return any results that came back
          referral.replyTo ! HomePageRecoFailed(RecoTimeoutMessage)
          Behaviors.stopped

        case x =>
          println(x)
          referral.replyTo ! HomePageRecoFailed(RecoUnknownError)
          Behaviors.stopped
      }

    initial()
  }


}
