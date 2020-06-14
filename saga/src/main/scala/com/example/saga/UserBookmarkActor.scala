package com.example.saga

import akka.actor.typed.scaladsl.{Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.sharding.typed.ShardingEnvelope
import com.datastax.driver.core.Session
import com.example.user.UserNodeEntity._

object UserBookmarkActor {
  sealed trait UserBookmarkCommand
  case class BookmarkUser(userId: UserId, targetUserId: String, replyTo: ActorRef[UserBookmarkReply]) extends UserBookmarkCommand
  case class WrappedUserEntityResponse(userEntityResponse: UserReply) extends UserBookmarkCommand

  sealed trait UserBookmarkReply
  case class UserBookmarkSagaSuccess(userId: String, updatedLabels: Map[String, Int]) extends UserBookmarkReply
  case class UserBookmarkSagaFailed(message: String) extends UserBookmarkReply

  def apply(
    userShardRegion: ActorRef[ShardingEnvelope[UserCommand[UserReply]]]
  )(implicit session: Session): Behavior[UserBookmarkCommand] =
    Behaviors.withTimers(timers => sagaBehaviour(userShardRegion, timers))

  def sagaBehaviour(
    userShardRegion: ActorRef[ShardingEnvelope[UserCommand[UserReply]]],
    timer: TimerScheduler[UserBookmarkCommand]
  )(implicit session: Session): Behavior[UserBookmarkCommand] = Behaviors.setup { cxt =>
    val userEntityResponseMapper: ActorRef[UserReply] =
      cxt.messageAdapter(rsp => WrappedUserEntityResponse(rsp))

    def initial(): Behavior[UserBookmarkCommand] =
      Behaviors.receiveMessagePartial {
        case bookmark: BookmarkUser =>
          userShardRegion ! ShardingEnvelope(
            bookmark.targetUserId,
            BookmarkedByRequest(
              userId = bookmark.targetUserId,
              bookmarkUser = bookmark.userId,
              replyTo = userEntityResponseMapper
            )
          )

          waitingForTargetUserReply(bookmark)
      }

    def waitingForTargetUserReply(bookmark: BookmarkUser): Behavior[UserBookmarkCommand] =
      Behaviors.receiveMessagePartial {
        case WrappedUserEntityResponse(wrapperUserReply: BookmarkedBySuccess) =>
          userShardRegion ! ShardingEnvelope(
            bookmark.userId,
            UserBookmarkRequest(
              userId = bookmark.userId,
              targetUserId = bookmark.targetUserId,
              labels = wrapperUserReply.labels,
              replyTo = userEntityResponseMapper
            )
          )
          waitingForBookmarkResponse(bookmark)

        case WrappedUserEntityResponse(wrapperUserReply: UserCommandFailed) =>
          bookmark.replyTo ! UserBookmarkSagaFailed(wrapperUserReply.error)
          Behaviors.stopped
      }

    def waitingForBookmarkResponse(bookmark: BookmarkUser): Behavior[UserBookmarkCommand] =
      Behaviors.receiveMessage {
        case WrappedUserEntityResponse(wrapperUserReply: UserBookmarkSuccess) =>
          bookmark.replyTo ! UserBookmarkSagaSuccess(wrapperUserReply.userId, wrapperUserReply.labels)
          Behaviors.stopped

        case x =>
          bookmark.replyTo ! UserBookmarkSagaFailed("did not receive UserBookmarkSuccess message")
          Behaviors.stopped
      }

    initial()
  }


}
