package com.example.saga

import akka.actor.typed.scaladsl.{Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.sharding.typed.ShardingEnvelope
import com.datastax.driver.core.Session
import com.example.saga.readside.SagaUserReadSideActor
import com.example.saga.readside.SagaUserReadSideActor.{RetrieveUsersQuery, SagaUserInfoResponse}
import com.example.user.UserNodeEntity._

object UserBookmarkActor {
  sealed trait UserBookmarkCommand
  case class BookmarkUser(userId: UserId, targetUserId: String, replyTo: ActorRef[UserBookmarkReply]) extends UserBookmarkCommand
  case class WrappedUserEntityResponse(userEntityResponse: UserReply) extends UserBookmarkCommand
  case class WrappedSagaUserActorResponse(usersResponse: SagaUserInfoResponse) extends UserBookmarkCommand

  sealed trait UserBookmarkReply
  case class UserBookmarkSagaSuccess(userId: String, updatedLabels: Map[String, Int], similarUsers: Set[UserUpdated]) extends UserBookmarkReply
  case class UserBookmarkSagaFailed(message: String) extends UserBookmarkReply

  def apply(
    userShardRegion: ActorRef[ShardingEnvelope[UserCommand[UserReply]]],
    sagaUserReadSideActor: ActorRef[SagaUserReadSideActor.SagaUserReadSideCommand]
  )(implicit session: Session): Behavior[UserBookmarkCommand] =
    Behaviors.withTimers(timers => sagaBehaviour(userShardRegion, sagaUserReadSideActor, timers))

  def sagaBehaviour(
    userShardRegion: ActorRef[ShardingEnvelope[UserCommand[UserReply]]],
    sagaUserReadSideActor: ActorRef[SagaUserReadSideActor.SagaUserReadSideCommand],
    timer: TimerScheduler[UserBookmarkCommand]
  )(implicit session: Session): Behavior[UserBookmarkCommand] = Behaviors.setup { cxt =>
    val userEntityResponseMapper: ActorRef[UserReply] =
      cxt.messageAdapter(rsp => WrappedUserEntityResponse(rsp))

    val sagaUserActorResponseMapper: ActorRef[SagaUserInfoResponse] =
      cxt.messageAdapter(rsp => WrappedSagaUserActorResponse(rsp))

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
          println(wrapperUserReply)

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
          sagaUserReadSideActor ! RetrieveUsersQuery(sagaUserActorResponseMapper, wrapperUserReply.similarUsers)
          waitingForUserInfoResponse(bookmark, wrapperUserReply)

        case x =>
          println(x)
          bookmark.replyTo ! UserBookmarkSagaFailed("did not receive UserBookmarkSuccess message")
          Behaviors.stopped
      }

    def waitingForUserInfoResponse(bookmark: BookmarkUser, wrapperUserReply: UserBookmarkSuccess): Behavior[UserBookmarkCommand] =
      Behaviors.receiveMessagePartial {
        case WrappedSagaUserActorResponse(sagaUserInfo: SagaUserInfoResponse) =>
          bookmark.replyTo ! UserBookmarkSagaSuccess(bookmark.userId, wrapperUserReply.labels, sagaUserInfo.list)
          Behaviors.stopped

        case x =>
          println(x)
          bookmark.replyTo ! UserBookmarkSagaFailed("did not receive SagaUserInfoResponse message")
          Behaviors.stopped
      }
    initial()
  }


}
