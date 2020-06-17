package com.example.saga

import akka.actor.typed.scaladsl.{Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.sharding.typed.ShardingEnvelope
import com.example.user.UserNodeEntity._

import scala.concurrent.duration._

object GetUserBookmarkedByActor {
  sealed trait GetUserBookmarkedByCommand
  case object CommandTimeout extends GetUserBookmarkedByCommand
  case class GetBookmarkedByWithUserInfo(userId: UserId, replyTo: ActorRef[GetUserBookmarkedByReply]) extends GetUserBookmarkedByCommand
  case class WrappedUserEntityResponse(userEntityResponse: UserReply) extends GetUserBookmarkedByCommand

  sealed trait GetUserBookmarkedByReply
  case class GetUserBookmarkedBySuccess(userId: String, bookmarkedUsers: Map[String, Map[String, String]]) extends GetUserBookmarkedByReply
  case class GetUserBookmarkedByFailed(message: String) extends GetUserBookmarkedByReply

  def apply(
    userShardRegion: ActorRef[ShardingEnvelope[UserCommand[UserReply]]]
  ): Behavior[GetUserBookmarkedByCommand] =
    Behaviors.withTimers(timers => getBookmarksInfoBehaviour(userShardRegion, timers))

  def getBookmarksInfoBehaviour(
    userShardRegion: ActorRef[ShardingEnvelope[UserCommand[UserReply]]],
    timer: TimerScheduler[GetUserBookmarkedByCommand]
  ): Behavior[GetUserBookmarkedByCommand] =
    Behaviors.setup { cxt =>
      val userEntityResponseMapper: ActorRef[UserReply] =
        cxt.messageAdapter(rsp => WrappedUserEntityResponse(rsp))

      def initial(): Behavior[GetUserBookmarkedByCommand] =
        Behaviors.receiveMessage {
          case bookmark: GetBookmarkedByWithUserInfo =>
            timer.startSingleTimer(CommandTimeout, 4 seconds)

            userShardRegion ! ShardingEnvelope(
              bookmark.userId,
              UserRetrievalCommand(
                userId = bookmark.userId,
                replyTo = userEntityResponseMapper
              )
            )
            waitingForUserReply(bookmark)

          case _ =>
            Behaviors.stopped
        }

    def waitingForUserReply(bookmark: GetBookmarkedByWithUserInfo): Behavior[GetUserBookmarkedByCommand] =
      Behaviors.receiveMessage {
        case WrappedUserEntityResponse(wrapperUserReply: UserInfo) =>
          wrapperUserReply.bookmarkedBy.foreach{ bookmarkedByUserId =>
            userShardRegion ! ShardingEnvelope(
              bookmarkedByUserId,
              UserRetrievalCommand(
                userId = bookmarkedByUserId,
                replyTo = userEntityResponseMapper
              )
            )
          }
          waitingForBookmarkUsersResponse(bookmark, wrapperUserReply, Set.empty)

        case WrappedUserEntityResponse(wrapperUserReply: UserCommandFailed) =>
          bookmark.replyTo ! GetUserBookmarkedByFailed(wrapperUserReply.error)
          Behaviors.stopped

        case x =>
          bookmark.replyTo ! GetUserBookmarkedByFailed("Could not get bookmarked users")
          Behaviors.stopped
      }

    def waitingForBookmarkUsersResponse(bookmark: GetBookmarkedByWithUserInfo, userInfo: UserInfo, receivedUserInfo: Set[UserInfo]): Behavior[GetUserBookmarkedByCommand] =
      Behaviors.receiveMessage {
        case WrappedUserEntityResponse(wrapperUserReply: UserInfo) =>
          val updatedReceivedUserInfo = receivedUserInfo + wrapperUserReply
          if(userInfo.bookmarkedBy.size == updatedReceivedUserInfo.size) {
            bookmark.replyTo ! GetUserBookmarkedBySuccess(bookmark.userId, updatedReceivedUserInfo.map(x => x.userId -> x.properties).toMap)

            Behaviors.stopped
          } else
          waitingForBookmarkUsersResponse(bookmark, userInfo, updatedReceivedUserInfo)

        case x =>
          bookmark.replyTo ! GetUserBookmarkedByFailed("Could not get bookmarked users")
          Behaviors.stopped
      }

    initial()
  }


}
