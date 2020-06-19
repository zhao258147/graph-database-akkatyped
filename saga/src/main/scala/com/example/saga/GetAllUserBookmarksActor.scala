package com.example.saga

import akka.actor.typed.scaladsl.{Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.sharding.typed.ShardingEnvelope
import com.example.user.UserNodeEntity._
import scala.concurrent.duration._

object GetAllUserBookmarksActor {
  sealed trait GetAllUserBookmarksCommand
  case object CommandTimeout extends GetAllUserBookmarksCommand
  case class GetAllBookmarksWithUserInfo(userId: UserId, replyTo: ActorRef[GetAllUserBookmarksReply]) extends GetAllUserBookmarksCommand
  case class WrappedUserEntityResponse(userEntityResponse: UserReply) extends GetAllUserBookmarksCommand

  sealed trait GetAllUserBookmarksReply
  case class GetUserBookmarksSuccess(userId: String, bookmarkedUsers: Map[String, Map[String, String]]) extends GetAllUserBookmarksReply
  case class GetUserBookmarksFailed(message: String) extends GetAllUserBookmarksReply

  def apply(
    userShardRegion: ActorRef[ShardingEnvelope[UserCommand[UserReply]]]
  ): Behavior[GetAllUserBookmarksCommand] =
    Behaviors.withTimers(timers => getBookmarksInfoBehaviour(userShardRegion, timers))

  def getBookmarksInfoBehaviour(
    userShardRegion: ActorRef[ShardingEnvelope[UserCommand[UserReply]]],
    timer: TimerScheduler[GetAllUserBookmarksCommand]
  ): Behavior[GetAllUserBookmarksCommand] =
    Behaviors.setup { cxt =>
      val userEntityResponseMapper: ActorRef[UserReply] =
        cxt.messageAdapter(rsp => WrappedUserEntityResponse(rsp))

      def initial(): Behavior[GetAllUserBookmarksCommand] =
        Behaviors.receiveMessage {
          case bookmark: GetAllBookmarksWithUserInfo =>
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

    def waitingForUserReply(bookmark: GetAllBookmarksWithUserInfo): Behavior[GetAllUserBookmarksCommand] =
      Behaviors.receiveMessage {
        case WrappedUserEntityResponse(wrapperUserReply: UserInfo) =>
          val waitingForUsers = wrapperUserReply.bookmarkedUsers ++ wrapperUserReply.bookmarkedBy

          if(waitingForUsers.isEmpty){
            bookmark.replyTo ! GetUserBookmarksSuccess(bookmark.userId, Map.empty)
            Behaviors.stopped
          } else {
            waitingForUsers.foreach{ bookmarkedUserId =>
              userShardRegion ! ShardingEnvelope(
                bookmarkedUserId,
                UserRetrievalCommand(
                  userId = bookmarkedUserId,
                  replyTo = userEntityResponseMapper
                )
              )
            }
            waitingForBookmarkUsersResponse(bookmark, wrapperUserReply.bookmarkedUsers, wrapperUserReply.bookmarkedBy, Set.empty)
          }


        case WrappedUserEntityResponse(wrapperUserReply: UserCommandFailed) =>
          bookmark.replyTo ! GetUserBookmarksFailed(wrapperUserReply.error)
          Behaviors.stopped

        case x =>
          bookmark.replyTo ! GetUserBookmarksFailed("Could not get bookmarked users")
          Behaviors.stopped
      }

    def waitingForBookmarkUsersResponse(bookmark: GetAllBookmarksWithUserInfo, bookmarkedUsers: Set[String], bookmarkedBy: Set[String], receivedUserInfo: Set[UserInfo]): Behavior[GetAllUserBookmarksCommand] =
      Behaviors.receiveMessage {
        case WrappedUserEntityResponse(wrapperUserReply: UserInfo) =>
          val updatedReceivedUserInfo = receivedUserInfo + wrapperUserReply
          val updatedUserIds = updatedReceivedUserInfo.map(_.userId)

          if(bookmarkedUsers.subsetOf(updatedUserIds) && bookmarkedBy.subsetOf(updatedUserIds)) {
            bookmark.replyTo ! GetUserBookmarksSuccess(
              bookmark.userId,
              updatedReceivedUserInfo.flatMap{ x: UserInfo =>
                if(bookmarkedBy.contains( x.userId)) {
                  Some(x.userId -> x.properties)
                } else {
                  if(x.autoReply) Some(x.userId -> x.properties)
                  else None
                }
               
              }.toMap
            )

            Behaviors.stopped
          } else
            waitingForBookmarkUsersResponse(bookmark, bookmarkedUsers, bookmarkedBy, updatedReceivedUserInfo)

        case x =>
          bookmark.replyTo ! GetUserBookmarksFailed("Could not get bookmarked users")
          Behaviors.stopped
      }

    initial()
  }


}
