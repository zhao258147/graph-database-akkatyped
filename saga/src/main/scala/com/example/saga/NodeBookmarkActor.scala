package com.example.saga

import akka.actor.typed.scaladsl.{Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.sharding.typed.ShardingEnvelope
import com.datastax.driver.core.Session
import com.example.graph.GraphNodeEntity._
import com.example.user.UserNodeEntity._

object NodeBookmarkActor {
  sealed trait NodeBookmarkCommand
  case class BookmarkNode(nodeId: NodeId, userId: UserId, replyTo: ActorRef[NodeBookmarkReply]) extends NodeBookmarkCommand
  case class WrappedNodeEntityResponse(nodeEntityResponse: GraphNodeCommandReply) extends NodeBookmarkCommand
  case class WrappedUserEntityResponse(userEntityResponse: UserReply) extends NodeBookmarkCommand

  sealed trait NodeBookmarkReply
  case class NodeBookmarkReqSuccess(userId: String, nodeId: String, updatedLabels: Map[String, Int]) extends NodeBookmarkReply
  case class NodeBookmarkReqFailed(message: String) extends NodeBookmarkReply

  def apply(
    graphShardRegion: ActorRef[ShardingEnvelope[GraphNodeCommand[GraphNodeCommandReply]]],
    userShardRegion: ActorRef[ShardingEnvelope[UserCommand[UserReply]]]
  )(implicit session: Session): Behavior[NodeBookmarkCommand] =
    Behaviors.withTimers(timers => sagaBehaviour(graphShardRegion, userShardRegion, timers))

  def sagaBehaviour(
    graphShardRegion: ActorRef[ShardingEnvelope[GraphNodeCommand[GraphNodeCommandReply]]],
    userShardRegion: ActorRef[ShardingEnvelope[UserCommand[UserReply]]],
    timer: TimerScheduler[NodeBookmarkCommand]
  )(implicit session: Session): Behavior[NodeBookmarkCommand] = Behaviors.setup { cxt =>
    val nodeEntityResponseMapper: ActorRef[GraphNodeCommandReply] =
      cxt.messageAdapter(rsp => WrappedNodeEntityResponse(rsp))

    val userEntityResponseMapper: ActorRef[UserReply] =
      cxt.messageAdapter(rsp => WrappedUserEntityResponse(rsp))

    def initial(): Behavior[NodeBookmarkCommand] =
      Behaviors.receiveMessage {
        case bookmark: BookmarkNode =>
          graphShardRegion ! ShardingEnvelope(
            bookmark.nodeId,
            NodeQuery(
              nodeId = bookmark.nodeId,
              replyTo = nodeEntityResponseMapper
            )
          )

          waitingForNodeReply(bookmark)

        case _ =>
          Behaviors.stopped
      }

    def waitingForNodeReply(bookmark: BookmarkNode): Behavior[NodeBookmarkCommand] =
      Behaviors.receiveMessage {
        case WrappedNodeEntityResponse(wrapperUserReply: NodeQueryResult) =>
          userShardRegion ! ShardingEnvelope(
            bookmark.userId,
            NodeBookmarkRequest(
              userId = bookmark.userId,
              nodeId = bookmark.nodeId,
              tags = wrapperUserReply.tags,
              replyTo = userEntityResponseMapper
            )
          )
          waitingForUserResponse(bookmark, wrapperUserReply)

        case WrappedUserEntityResponse(wrapperUserReply: UserCommandFailed) =>
          bookmark.replyTo ! NodeBookmarkReqFailed(wrapperUserReply.error)
          Behaviors.stopped

        case x =>
//          println(x)
          bookmark.replyTo ! NodeBookmarkReqFailed("Did not receive NodeQueryResult message")
          Behaviors.stopped
      }

    def waitingForUserResponse(bookmark: BookmarkNode, nodeInfo: NodeQueryResult): Behavior[NodeBookmarkCommand] =
      Behaviors.receiveMessage {
        case WrappedUserEntityResponse(wrapperUserReply: NodeBookmarkSuccess) =>
          bookmark.replyTo ! NodeBookmarkReqSuccess(bookmark.userId, bookmark.nodeId, wrapperUserReply.labels)
          Behaviors.stopped

        case x =>
//          println(x)
          bookmark.replyTo ! NodeBookmarkReqFailed("Did not receive NodeBookmarkSuccess message")
          Behaviors.stopped
      }

    initial()
  }


}
