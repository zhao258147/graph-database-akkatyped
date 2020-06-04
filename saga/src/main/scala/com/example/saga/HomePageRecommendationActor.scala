package com.example.saga

import akka.actor.typed.scaladsl.{Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.sharding.typed.ShardingEnvelope
import com.datastax.driver.core.Session
import com.example.graph.readside.ClickReadSideActor
import com.example.graph.readside.ClickReadSideActor.{TrendingNodesCommand, TrendingNodesResponse}
import com.example.saga.NodeRecommendationActor._
import com.example.saga.readside.SagaNodeReadSideActor.{apply => _, _}
import com.example.saga.readside.{SagaNodeReadSideActor, SagaTrendingNodesActor}
import com.example.user.UserNodeEntity._

object HomePageRecommendationActor {
  sealed trait HomePageRecommendationCommand
  case object HomePageRecommendationTimeout extends HomePageRecommendationCommand
  case class HomePageRecommendation(userId: UserId, userLabels: Map[String, Int], replyTo: ActorRef[HomePageRecommendationReply]) extends HomePageRecommendationCommand
  case class WrappedUserEntityResponse(userEntityResponse: UserReply) extends HomePageRecommendationCommand
  case class WrappedClickActorResponse(trendingResponse: TrendingNodesResponse) extends HomePageRecommendationCommand
  case class WrappedSagaNodeActorResponse(nodesResponse: SagaNodeReadSideResponse) extends HomePageRecommendationCommand

  sealed trait HomePageRecommendationReply
  case class HomePageRecoReply(userId: String, relevant: Seq[RecoWithNodeInfo], overallRanking: Seq[RecoWithNodeInfo], rankingByType: Map[String, Seq[RecoWithNodeInfo]], neighbourHistory: Seq[RecoWithNodeInfo], neighbourUsers: Seq[UserDisplayInfo]) extends HomePageRecommendationReply
  case class HomePageRecoFailed(message: String) extends HomePageRecommendationReply

  private def checkAllResps(
    userEntityResponseMapper: ActorRef[UserReply],
    referral: HomePageRecommendation,
    userShardRegion: ActorRef[ShardingEnvelope[UserCommand[UserReply]]],
    userHistoryResponse: Option[UserHistoryResponse],
    nodeActorResponse: Option[SagaNodesQueryResponse],
    trendingNodesResponse: Option[TrendingNodesResponse]
  ): Option[(UserHistoryResponse, SagaNodesQueryResponse, TrendingNodesResponse, Boolean)] = for {
    nodeResp <- nodeActorResponse
    trendingResp <- trendingNodesResponse
    userResp <- userHistoryResponse
  } yield {
//    println(userResp)
    userResp.neighbours.foreach{ userId =>
      userShardRegion ! ShardingEnvelope(
        userId,
        NeighbouringViewsRequest(userId, userEntityResponseMapper)
      )
    }

    (userResp, nodeResp, trendingResp, userResp.neighbours.isEmpty)
  }

  def apply(
    userShardRegion: ActorRef[ShardingEnvelope[UserCommand[UserReply]]],
    clickReadSideActor: ActorRef[ClickReadSideActor.ClickStatCommands],
    sagaNodeReadSideActor: ActorRef[SagaNodeReadSideActor.SagaNodeReadSideCommand]
  )(implicit session: Session): Behavior[HomePageRecommendationCommand] =
    Behaviors.withTimers(timers => sagaBehaviour(userShardRegion, clickReadSideActor, sagaNodeReadSideActor, timers))

  def sagaBehaviour(
    userShardRegion: ActorRef[ShardingEnvelope[UserCommand[UserReply]]],
    clickReadSideActor: ActorRef[ClickReadSideActor.ClickStatCommands],
    sagaNodeReadSideActor: ActorRef[SagaNodeReadSideActor.SagaNodeReadSideCommand],
    timer: TimerScheduler[HomePageRecommendationCommand]
  )(implicit session: Session): Behavior[HomePageRecommendationCommand] = Behaviors.setup { cxt =>
    val userEntityResponseMapper: ActorRef[UserReply] =
      cxt.messageAdapter(rsp => WrappedUserEntityResponse(rsp))

    val trendingActorResponseMapper: ActorRef[TrendingNodesResponse] =
      cxt.messageAdapter(rsp => WrappedClickActorResponse(rsp))

    val sagaNodeActorResponseMapper: ActorRef[SagaNodeReadSideResponse] =
      cxt.messageAdapter(rsp => WrappedSagaNodeActorResponse(rsp))

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
//          println(x)
          Behaviors.stopped
      }

    def waitingForRecommendationReply(referral: HomePageRecommendation, userHistoryResponse: Option[UserHistoryResponse], nodeActorResponse: Option[SagaNodesQueryResponse], trendingNodesResponse: Option[TrendingNodesResponse]): Behavior[HomePageRecommendationCommand] =
      Behaviors.receiveMessage {
        case WrappedClickActorResponse(wrappedTrendingReply) =>
          checkAllResps(userEntityResponseMapper, referral, userShardRegion, userHistoryResponse, nodeActorResponse, Some(wrappedTrendingReply))
            .map{
              case (userResp, nodeResp, trendingResp, noNeighbours) =>
                if(noNeighbours) {
                  sagaNodeReadSideActor ! RetrieveNodesQuery(Map.empty, Map.empty, nodeResp.list -- userResp.viewed, trendingResp.overallRanking, Map.empty, trendingResp.rankingByType, sagaNodeActorResponseMapper)
                  waitingForFinalNeighbourViewsFilter(referral, Seq.empty)
                } else waitingForNeighbourViewsReply(referral, nodeResp.list, trendingResp, userResp.neighbours, Map.empty, userResp.viewed, Seq.empty)
            }.getOrElse(waitingForRecommendationReply(referral, userHistoryResponse, nodeActorResponse, Some(wrappedTrendingReply)))

        case WrappedSagaNodeActorResponse(wrappedNodeReply: SagaNodesQueryResponse) =>
          checkAllResps(userEntityResponseMapper, referral, userShardRegion, userHistoryResponse, Some(wrappedNodeReply), trendingNodesResponse)
            .map{
              case (userResp, nodeResp, trendingResp, noNeighbours) =>
                if(noNeighbours) {
                  sagaNodeReadSideActor ! RetrieveNodesQuery(Map.empty, Map.empty, nodeResp.list -- userResp.viewed, trendingResp.overallRanking, Map.empty, trendingResp.rankingByType, sagaNodeActorResponseMapper)
                  waitingForFinalNeighbourViewsFilter(referral, Seq.empty)
                } else waitingForNeighbourViewsReply(referral, nodeResp.list, trendingResp, userResp.neighbours, Map.empty, userResp.viewed, Seq.empty)
            }.getOrElse(waitingForRecommendationReply(referral, userHistoryResponse, Some(wrappedNodeReply), trendingNodesResponse))

        case WrappedUserEntityResponse(wrapperUserReply: UserHistoryResponse) =>
          checkAllResps(userEntityResponseMapper, referral, userShardRegion, Some(wrapperUserReply), nodeActorResponse, trendingNodesResponse)
            .map{
              case (userResp, nodeResp, trendingResp, noNeighbours) =>
                if(noNeighbours) {
                  sagaNodeReadSideActor ! RetrieveNodesQuery(Map.empty, Map.empty, nodeResp.list -- userResp.viewed, trendingResp.overallRanking, Map.empty, trendingResp.rankingByType, sagaNodeActorResponseMapper)
                  waitingForFinalNeighbourViewsFilter(referral, Seq.empty)
                } else waitingForNeighbourViewsReply(referral, nodeResp.list, trendingResp, userResp.neighbours, Map.empty, userResp.viewed, Seq.empty)
            }.getOrElse(waitingForRecommendationReply(referral, Some(wrapperUserReply), nodeActorResponse, trendingNodesResponse))

        case HomePageRecommendationTimeout =>
//          println(s"(userHistoryResponse, nodeActorResponse, trendingNodesResponse) (${userHistoryResponse.isDefined}, ${nodeActorResponse.isDefined}, ${trendingNodesResponse.isDefined})")

          //TODO: implement this to return any results that came back
          referral.replyTo ! HomePageRecoFailed(RecoTimeoutMessage)
          Behaviors.stopped

        case x =>
//          println(x)
          referral.replyTo ! HomePageRecoFailed(RecoUnknownError)
          Behaviors.stopped
      }

    def waitingForNeighbourViewsReply(referral: HomePageRecommendation, relevantNodes: Map[String, Int], trendingResp: TrendingNodesResponse, neighbours: Set[String], viewsCollected: Map[String, Int], userViewHistory: Seq[String], neighbourUsers: Seq[UserDisplayInfo]): Behavior[HomePageRecommendationCommand] =
      Behaviors.receiveMessage {
        case WrappedUserEntityResponse(wrapperUserReply: NeighbouringViews) =>
//          println(wrapperUserReply)
          val neighboursLeft = neighbours - wrapperUserReply.userId
          val updatedViews = viewsCollected ++ wrapperUserReply.viewed.map{ nodeId =>
            nodeId -> (viewsCollected.getOrElse(nodeId, 0) + 1)
          }

          val updatedNeighbourUsers = UserDisplayInfo(wrapperUserReply.userId, wrapperUserReply.userType, wrapperUserReply.properties, wrapperUserReply.labels) +: neighbourUsers

          if(neighboursLeft.isEmpty) {
            sagaNodeReadSideActor ! RetrieveNodesQuery(Map.empty, Map.empty, relevantNodes -- userViewHistory, trendingResp.overallRanking, updatedViews -- userViewHistory, trendingResp.rankingByType, sagaNodeActorResponseMapper)

            waitingForFinalNeighbourViewsFilter(referral, updatedNeighbourUsers)
          } else waitingForNeighbourViewsReply(referral, relevantNodes, trendingResp, neighboursLeft, updatedViews, userViewHistory, updatedNeighbourUsers)

        case HomePageRecommendationTimeout =>
          //TODO: implement this to return any results that came back
          referral.replyTo ! HomePageRecoFailed(RecoTimeoutMessage)
          Behaviors.stopped

        case x =>
//          println(x)
          referral.replyTo ! HomePageRecoFailed(RecoUnknownError)
          Behaviors.stopped
      }

    def waitingForFinalNeighbourViewsFilter(referral: HomePageRecommendation, neighbourUsers: Seq[UserDisplayInfo]): Behavior[HomePageRecommendationCommand] =
      Behaviors.receiveMessage {
        case WrappedSagaNodeActorResponse(sagaNodeReply: SagaNodesInfoResponse) =>
          referral.replyTo ! HomePageRecoReply(referral.userId, sagaNodeReply.relevant, sagaNodeReply.trending, sagaNodeReply.trendingByTag, sagaNodeReply.neighbourHistory, neighbourUsers)
          Behaviors.stopped

        case HomePageRecommendationTimeout =>
          //TODO: implement this to return any results that came back
          referral.replyTo ! HomePageRecoFailed(RecoTimeoutMessage)
          Behaviors.stopped

        case x =>
//          println(x)
          referral.replyTo ! HomePageRecoFailed(RecoUnknownError)
          Behaviors.stopped
      }

    initial()
  }


}
