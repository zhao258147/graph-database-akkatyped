package com.example.saga
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import com.example.graph.readside.ClickReadSideActor
import com.example.graph.readside.ClickReadSideActor.{TrendingNodesCommand, TrendingNodesResponse}
import com.example.saga.readside.SagaNodeReadSideActor
import com.example.saga.readside.SagaNodeReadSideActor.{RecoWithNodeInfo, RetrieveNodesQuery, SagaNodeReadSideResponse, SagaNodesInfoResponse}

object NodeTrendingActor {
  sealed trait NodeTrendingCommand
  case class GetTrendingNodes(replyTo: ActorRef[NodeTrendingReply]) extends NodeTrendingCommand
  case class WrappedClickActorResponse(trendingResponse: TrendingNodesResponse) extends NodeTrendingCommand
  case class WrappedSagaNodeActorResponse(nodesResponse: SagaNodeReadSideResponse) extends NodeTrendingCommand

  sealed trait NodeTrendingReply
  case class NodeBookmarkReqSuccess(trending: Seq[RecoWithNodeInfo]) extends NodeTrendingReply
  case class NodeBookmarkReqFailed(message: String) extends NodeTrendingReply

  def apply(
    clickReadSideActor: ActorRef[ClickReadSideActor.ClickStatCommands],
    sagaNodeReadSideActor: ActorRef[SagaNodeReadSideActor.SagaNodeReadSideCommand]
  ): Behavior[NodeTrendingCommand] = Behaviors.setup { cxt =>
    val trendingActorResponseMapper: ActorRef[TrendingNodesResponse] =
      cxt.messageAdapter(rsp => WrappedClickActorResponse(rsp))

    val sagaNodeActorResponseMapper: ActorRef[SagaNodeReadSideResponse] =
      cxt.messageAdapter(rsp => WrappedSagaNodeActorResponse(rsp))

    def initial(): Behavior[NodeTrendingCommand] =
      Behaviors.receiveMessage {
        case referral: GetTrendingNodes =>
          clickReadSideActor ! TrendingNodesCommand(trendingActorResponseMapper)
          waitingForTrendingNodes(referral)

        case x =>
          cxt.log.debug(x.toString)
          Behaviors.stopped
      }

    def waitingForTrendingNodes(referral: GetTrendingNodes): Behavior[NodeTrendingCommand] =
      Behaviors.receiveMessage{
        case WrappedClickActorResponse(trendingNodes: TrendingNodesResponse) =>
          sagaNodeReadSideActor ! RetrieveNodesQuery(Map.empty, Map.empty, Map.empty, trendingNodes.overallRanking, Map.empty, Map.empty, sagaNodeActorResponseMapper)

          waitingForNodeInfo(referral)

        case x =>
          cxt.log.debug(x.toString)
          Behaviors.stopped
      }

    def waitingForNodeInfo(referral: GetTrendingNodes): Behavior[NodeTrendingCommand] =
      Behaviors.receiveMessage{
        case WrappedSagaNodeActorResponse(info: SagaNodesInfoResponse) =>
          referral.replyTo ! NodeBookmarkReqSuccess(info.trending)

          Behaviors.stopped

        case x =>
          cxt.log.debug(x.toString)
          Behaviors.stopped
      }

    initial()
  }


}