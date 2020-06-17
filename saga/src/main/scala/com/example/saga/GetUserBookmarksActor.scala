package com.example.saga

import akka.actor.typed.scaladsl.{Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.sharding.typed.ShardingEnvelope
import com.example.user.UserNodeEntity._
import scala.concurrent.duration._

object GetUserBookmarksActor {
  sealed trait GetUserBookmarksCommand
  case object CommandTimeout extends GetUserBookmarksCommand
  case class GetBookmarksWithUserInfo(userId: UserId, replyTo: ActorRef[GetUserBookmarksReply]) extends GetUserBookmarksCommand
  case class WrappedUserEntityResponse(userEntityResponse: UserReply) extends GetUserBookmarksCommand

  sealed trait GetUserBookmarksReply
  case class GetUserBookmarksSuccess(userId: String, bookmarkedUsers: Map[String, Map[String, String]]) extends GetUserBookmarksReply
  case class GetUserBookmarksFailed(message: String) extends GetUserBookmarksReply

  def apply(
    userShardRegion: ActorRef[ShardingEnvelope[UserCommand[UserReply]]]
  ): Behavior[GetUserBookmarksCommand] =
    Behaviors.withTimers(timers => getBookmarksInfoBehaviour(userShardRegion, timers))

  def getBookmarksInfoBehaviour(
    userShardRegion: ActorRef[ShardingEnvelope[UserCommand[UserReply]]],
    timer: TimerScheduler[GetUserBookmarksCommand]
  ): Behavior[GetUserBookmarksCommand] =
    Behaviors.setup { cxt =>
      val userEntityResponseMapper: ActorRef[UserReply] =
        cxt.messageAdapter(rsp => WrappedUserEntityResponse(rsp))

      def initial(): Behavior[GetUserBookmarksCommand] =
        Behaviors.receiveMessage {
          case bookmark: GetBookmarksWithUserInfo =>
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

    def waitingForUserReply(bookmark: GetBookmarksWithUserInfo): Behavior[GetUserBookmarksCommand] =
      Behaviors.receiveMessage {
        case WrappedUserEntityResponse(wrapperUserReply: UserInfo) =>
          wrapperUserReply.bookmarkedUsers.foreach{ bookmarkedUserId =>
            userShardRegion ! ShardingEnvelope(
              bookmarkedUserId,
              UserRetrievalCommand(
                userId = bookmarkedUserId,
                replyTo = userEntityResponseMapper
              )
            )
          }
          waitingForBookmarkUsersResponse(bookmark, wrapperUserReply, Set.empty)

        case WrappedUserEntityResponse(wrapperUserReply: UserCommandFailed) =>
          bookmark.replyTo ! GetUserBookmarksFailed(wrapperUserReply.error)
          Behaviors.stopped

        case x =>
          bookmark.replyTo ! GetUserBookmarksFailed("Could not get bookmarked users")
          Behaviors.stopped
      }

    def waitingForBookmarkUsersResponse(bookmark: GetBookmarksWithUserInfo, userInfo: UserInfo, receivedUserInfo: Set[UserInfo]): Behavior[GetUserBookmarksCommand] =
      Behaviors.receiveMessage {
        case WrappedUserEntityResponse(wrapperUserReply: UserInfo) =>
          val updatedReceivedUserInfo = receivedUserInfo + wrapperUserReply
          if(userInfo.bookmarkedUsers.size == updatedReceivedUserInfo.size) {
            bookmark.replyTo ! GetUserBookmarksSuccess(bookmark.userId, updatedReceivedUserInfo.map(x => x.userId -> x.properties).toMap)

            Behaviors.stopped
          } else
            waitingForBookmarkUsersResponse(bookmark, userInfo, updatedReceivedUserInfo)

        case x =>
          bookmark.replyTo ! GetUserBookmarksFailed("Could not get bookmarked users")
          Behaviors.stopped
      }

    initial()
  }


}
