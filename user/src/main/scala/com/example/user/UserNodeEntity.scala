package com.example.user

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior, ReplyEffect, RetentionCriteria}
import com.example.user.config.UserEntityParams
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}

object UserNodeEntity {
  type UserId = String
  val UserStateErrorMessage = "User entity not in the right state"

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[NodeReferralBias], name = "NodeReferralBias"),
      new JsonSubTypes.Type(value = classOf[NodeSearchBias], name = "NodeSearchBias")
    )
  )
  sealed trait NodeVisitBias
  case class NodeReferralBias() extends NodeVisitBias
  case class NodeSearchBias() extends NodeVisitBias

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[CreateUserCommand], name = "CreateUserCommand"),
      new JsonSubTypes.Type(value = classOf[UpdateUserCommand], name = "UpdateUserCommand"),
      new JsonSubTypes.Type(value = classOf[UpdateUserPropertiesCommand], name = "UpdateUserPropertiesCommand"),
      new JsonSubTypes.Type(value = classOf[UpdateUserLabelsCommand], name = "UpdateUserLabelsCommand"),
      new JsonSubTypes.Type(value = classOf[NodeVisitRequest], name = "NodeVisitRequest"),
      new JsonSubTypes.Type(value = classOf[UserHistoryRetrivalRequest], name = "UserHistoryRetrivalRequest"),
      new JsonSubTypes.Type(value = classOf[NodeBookmarkRequest], name = "NodeBookmarkRequest"),
      new JsonSubTypes.Type(value = classOf[RemoveNodeBookmarkRequest], name = "RemoveNodeBookmarkRequest"),
      new JsonSubTypes.Type(value = classOf[UserBookmarkRequest], name = "UserBookmarkRequest"),
      new JsonSubTypes.Type(value = classOf[RemoveUserBookmarkRequest], name = "RemoveUserBookmarkRequest"),
      new JsonSubTypes.Type(value = classOf[BookmarkedByRequest], name = "BookmarkedByRequest"),
      new JsonSubTypes.Type(value = classOf[RemoveBookmarkedByRequest], name = "RemoveBookmarkedByRequest"),
      new JsonSubTypes.Type(value = classOf[SetAutoReplyCommand], name = "SetAutoReplyCommand"),
      new JsonSubTypes.Type(value = classOf[UserRetrievalCommand], name = "UserRetrievalCommand"),
      new JsonSubTypes.Type(value = classOf[NeighbouringViewsRequest], name = "NeighbouringViewsRequest"),
      new JsonSubTypes.Type(value = classOf[UserEntityParamsUpdate], name = "UserEntityParamsUpdate")
    )
  )
  sealed trait UserCommand[Reply <: UserReply] {
    val userId: UserId
    def replyTo: ActorRef[Reply]
  }

  case class CreateUserCommand(userId: UserId, userType: String, properties: Map[String, String], labels: Map[String, Int], replyTo: ActorRef[UserReply]) extends UserCommand[UserReply]
  case class UpdateUserCommand(userId: UserId, properties: Map[String, String], labels: Map[String, Int], replyTo: ActorRef[UserReply]) extends UserCommand[UserReply]
  case class UpdateUserPropertiesCommand(userId: UserId, properties: Map[String, String], replyTo: ActorRef[UserReply]) extends UserCommand[UserReply]
  case class UpdateUserLabelsCommand(userId: UserId, labels: Map[String, Int], replyTo: ActorRef[UserReply]) extends UserCommand[UserReply]
  case class NodeVisitRequest(userId: UserId, nodeId: String, tags: Map[String, Int], similarUsers: Map[String, Map[String, Int]], bias: NodeVisitBias = NodeReferralBias(), replyTo: ActorRef[UserReply]) extends UserCommand[UserReply]
  case class UserHistoryRetrivalRequest(userId: UserId, replyTo: ActorRef[UserReply]) extends UserCommand[UserReply]

  case class NodeBookmarkRequest(userId: UserId, nodeId: String, tags: Map[String, Int], replyTo: ActorRef[UserReply]) extends UserCommand[UserReply]
  case class RemoveNodeBookmarkRequest(userId: UserId, nodeId: String, replyTo: ActorRef[UserReply]) extends UserCommand[UserReply]

  case class UserBookmarkRequest(userId: UserId, targetUserId: String, labels: Map[String, Int], autoReply: Boolean, replyTo: ActorRef[UserReply]) extends UserCommand[UserReply]
  case class RemoveUserBookmarkRequest(userId: UserId, targetUserId: String, replyTo: ActorRef[UserReply]) extends UserCommand[UserReply]

  case class BookmarkedByRequest(userId: UserId, bookmarkUser: String, replyTo: ActorRef[UserReply]) extends UserCommand[UserReply]
  case class RemoveBookmarkedByRequest(userId: UserId, bookmarkUser: String, replyTo: ActorRef[UserReply]) extends UserCommand[UserReply]

  case class SetAutoReplyCommand(userId: UserId, autoReply: Boolean, replyTo: ActorRef[UserReply]) extends UserCommand[UserReply]
  case class UserRetrievalCommand(userId: UserId, replyTo: ActorRef[UserReply]) extends UserCommand[UserReply]
  case class NeighbouringViewsRequest(userId: UserId, replyTo: ActorRef[UserReply]) extends UserCommand[UserReply]
  case class UserEntityParamsUpdate(userId: UserId, numberOfSimilarUsers: Option[Int], numberOfViewsToCheck: Option[Int], labelWeightFilter: Option[Int], nodeBookmarkBias: Option[Int], userBookmarkBias: Option[Int], nodeVisitBias: Option[Int], replyTo: ActorRef[UserReply]) extends UserCommand[UserReply]

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[UserCommandSuccess], name = "UserCommandSuccess"),
      new JsonSubTypes.Type(value = classOf[NodeVisitRequestSuccess], name = "NodeVisitRequestSuccess"),
      new JsonSubTypes.Type(value = classOf[UserHistoryResponse], name = "UserHistoryResponse"),
      new JsonSubTypes.Type(value = classOf[UserCommandFailed], name = "UserCommandFailed"),
      new JsonSubTypes.Type(value = classOf[UserInfo], name = "UserInfo"),
      new JsonSubTypes.Type(value = classOf[BookmarkedBySuccess], name = "BookmarkedBySuccess"),
      new JsonSubTypes.Type(value = classOf[UserBookmarkSuccess], name = "UserBookmarkSuccess"),
      new JsonSubTypes.Type(value = classOf[NodeBookmarkSuccess], name = "NodeBookmarkSuccess"),
      new JsonSubTypes.Type(value = classOf[NeighbouringViews], name = "NeighbouringViews")
    )
  )
  sealed trait UserReply {
    val userId: UserId
  }
  case class UserCommandSuccess(userId: UserId, labels: Map[String, Int]) extends UserReply
  case class NodeVisitRequestSuccess(userId: UserId, updatedLabels: Map[String, Int], neighbours: Set[UserId], recentViews: Seq[String]) extends UserReply
  case class UserHistoryResponse(userId: UserId, neighbours: Set[UserId], viewed: Seq[String]) extends UserReply
  case class UserCommandFailed(userId: UserId, error: String) extends UserReply
  case class UserInfo(userId: UserId, userType: String, properties: Map[String, String], labels: Map[String, Int], viewed: Seq[String], bookmarkedNodes: Set[String], bookmarkedUsers: Set[String], bookmarkedBy: Set[String], similarUsers: Map[String, Int], autoReply: Boolean) extends UserReply
  case class BookmarkedBySuccess(userId: UserId, labels: Map[String, Int], autoReply: Boolean) extends UserReply
  case class UserBookmarkSuccess(userId: UserId, labels: Map[String, Int]) extends UserReply
  case class NodeBookmarkSuccess(userId: UserId, nodeId: String, labels: Map[String, Int]) extends UserReply
  case class NeighbouringViews(userId: UserId, userType: String, properties: Map[String, String], labels: Map[String, Int], viewed: Seq[String]) extends UserReply

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[UserUpdated], name = "UserUpdated"),
      new JsonSubTypes.Type(value = classOf[NodeBookmarked], name = "NodeBookmarked"),
      new JsonSubTypes.Type(value = classOf[UserBookmarked], name = "UserBookmarked"),
      new JsonSubTypes.Type(value = classOf[BookmarkedBy], name = "BookmarkedBy"),
      new JsonSubTypes.Type(value = classOf[RemoveNodeBookmarked], name = "RemoveNodeBookmarked"),
      new JsonSubTypes.Type(value = classOf[RemoveUserBookmarked], name = "RemoveUserBookmarked"),
      new JsonSubTypes.Type(value = classOf[RemoveBookmarkedBy], name = "RemoveBookmarkedBy"),
      new JsonSubTypes.Type(value = classOf[UserRequest], name = "UserRequest"),
      new JsonSubTypes.Type(value = classOf[UserAutoReplyUpdate], name = "UserAutoReplyUpdate"),
      new JsonSubTypes.Type(value = classOf[ParamsUpdate], name = "ParamsUpdate")
    )
  )
  sealed trait UserEvent {
    val ts: Long
  }
  case class UserUpdated(userId: UserId, userType: String, properties: Map[String, String], labels: Map[String, Int], ts: Long = System.currentTimeMillis()) extends UserEvent

  case class NodeBookmarked(userId: UserId, nodeId: String, tags: Map[String, Int], ts: Long = System.currentTimeMillis()) extends UserEvent
  case class UserBookmarked(userId: UserId, targetUserId: String, labels: Map[String, Int], autoReply: Boolean, ts: Long = System.currentTimeMillis()) extends UserEvent
  case class BookmarkedBy(userId: UserId, bookmarkUser: String, ts: Long = System.currentTimeMillis()) extends UserEvent

  case class RemoveNodeBookmarked(userId: UserId, nodeId: String, ts: Long = System.currentTimeMillis()) extends UserEvent
  case class RemoveUserBookmarked(userId: UserId, targetUserId: String, ts: Long = System.currentTimeMillis()) extends UserEvent
  case class RemoveBookmarkedBy(userId: UserId, bookmarkUser: String, ts: Long = System.currentTimeMillis()) extends UserEvent

  case class UserAutoReplyUpdate(userId: UserId, autoReply: Boolean, ts: Long = System.currentTimeMillis()) extends UserEvent
  case class UserRequest(nodeId: String, tags: Map[String, Int], similarUsers: Map[String, Map[String, Int]], bias: NodeVisitBias = NodeReferralBias(), ts: Long = System.currentTimeMillis()) extends UserEvent
  case class ParamsUpdate(userId: UserId, numberOfSimilarUsers: Option[Int], numberOfViewsToCheck: Option[Int], labelWeightFilter: Option[Int], nodeBookmarkBias: Option[Int], userBookmarkBias: Option[Int], nodeVisitBias: Option[Int], ts: Long = System.currentTimeMillis()) extends UserEvent

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[EmptyUserState], name = "EmptyUserState"),
      new JsonSubTypes.Type(value = classOf[CreatedUserState], name = "CreatedUserState")
    )
  )
  sealed trait UserState
  case class EmptyUserState() extends UserState
  case class CreatedUserState(
    userId: UserId,
    userType: String,
    properties: Map[String, String],
    labels: Map[String, Int],
    bookmarkedNodes: Map[String, NodeBookmarked],
    bookmarkedUsers: Map[String, UserBookmarked],
    bookmarkedBy: Map[String, BookmarkedBy],
    similarUsers: Map[String, Map[String, Int]],
    viewed: Seq[String],
    autoReply: Boolean = true,
    numberOfSimilarUsersOpt: Option[Int] = None,
    numberOfViewsToCheckOpt: Option[Int] = None,
    labelWeightFilterOpt: Option[Int] = None,
    nodeBookmarkBiasOpt: Option[Int] = None,
    userBookmarkBiasOpt: Option[Int] = None,
    nodeVisitBiasOpt: Option[Int] = None,
    nodeSearchBiasOpt: Option[Int] = None
  ) extends UserState {
    def numberOfSimilarUsers(implicit params: UserEntityParams): Int = numberOfSimilarUsersOpt.getOrElse(params.numberOfSimilarUsers)
    def numberOfViewsToCheck(implicit params: UserEntityParams): Int = numberOfViewsToCheckOpt.getOrElse(params.numberOfViewsToCheck)
    def labelWeightFilter(implicit params: UserEntityParams): Int = labelWeightFilterOpt.getOrElse(params.labelWeightFilter)
    def nodeBookmarkBias(implicit params: UserEntityParams): Int = nodeBookmarkBiasOpt.getOrElse(params.nodeBookmarkBias)
    def userBookmarkBias(implicit params: UserEntityParams): Int = userBookmarkBiasOpt.getOrElse(params.userBookmarkBias)
    def nodeVisitBias(implicit params: UserEntityParams): Int = nodeVisitBiasOpt.getOrElse(params.nodeVisitBias)
    def nodeSearchBias(implicit params: UserEntityParams): Int = nodeSearchBiasOpt.getOrElse(params.nodeSearchBias)
  }

  private def rankSimilarUsers(withNewSimilarUsers: Map[String, Map[String, Int]], updatedLabels: Map[String, Int]) =
    withNewSimilarUsers.map{
      case (userId, labels) =>
        userId -> updatedLabels.foldLeft(0){
          case (acc, (label, labelWeight)) =>
            acc + Math.min(labels.getOrElse(label, 0), labelWeight)
        }
    }.toSeq.sortWith(_._2 > _._2)

  private def commandHandler(context: ActorContext[UserCommand[UserReply]])(implicit params: UserEntityParams): (UserState, UserCommand[UserReply]) => ReplyEffect[UserEvent, UserState] = {
    (state, command) =>
//      context.log.debug(command.toString)
//      context.log.debug(state.toString)
      state match {
        case empty: EmptyUserState =>
          command match {
            case cmd: CreateUserCommand =>
              Effect
                .persist(UserUpdated(cmd.userId, cmd.userType, cmd.properties, cmd.labels))
                .thenReply(cmd.replyTo)(_ => UserCommandSuccess(cmd.userId, cmd.labels))

            case cmd =>
              Effect.reply(cmd.replyTo)(UserCommandFailed(cmd.userId, "User does not exist"))
          }

        case state: CreatedUserState =>
          command match {
            case cmd: CreateUserCommand =>
              println(cmd)
              println(state)
              if(state.properties == cmd.properties)
                Effect.reply(cmd.replyTo)(UserCommandSuccess(cmd.userId, state.labels))
              else
                Effect
                  .persist(UserUpdated(cmd.userId, cmd.userType, cmd.properties, state.labels))
                  .thenReply(cmd.replyTo)(_ => UserCommandSuccess(cmd.userId, state.labels))

            case update: UpdateUserCommand =>
              Effect
                .persist(UserUpdated(update.userId, state.userType, state.properties ++ update.properties, update.labels))
                .thenReply(update.replyTo)(_ => UserCommandSuccess(update.userId, state.labels))

            case update: UserEntityParamsUpdate =>
              Effect
                .persist(ParamsUpdate(update.userId, update.numberOfSimilarUsers, update.numberOfViewsToCheck, update.labelWeightFilter, update.nodeBookmarkBias, update.userBookmarkBias, update.nodeVisitBias))
                .thenReply(update.replyTo)(_ => UserCommandSuccess(update.userId, state.labels))

            case update: UpdateUserPropertiesCommand =>
              Effect
                .persist(UserUpdated(update.userId, state.userType, update.properties, state.labels))
                .thenReply(update.replyTo)(_ => UserCommandSuccess(update.userId, state.labels))

            case update: UpdateUserLabelsCommand =>
              Effect
                .persist(UserUpdated(update.userId, state.userType, state.properties, update.labels))
                .thenReply(update.replyTo)(_ => UserCommandSuccess(update.userId, state.labels))

            case req: NodeVisitRequest =>
              val viewed = state.viewed.take(params.numberOfViewsToCheck)
              val evt = UserRequest(req.nodeId, req.tags, req.similarUsers.filterNot(_._1 == state.userId), req.bias)
//              context.log.debug(evt.toString)
              Effect
                .persist(evt)
                .thenReply(req.replyTo){
                  case updatedState: CreatedUserState =>
                    NodeVisitRequestSuccess(req.userId, updatedState.labels, updatedState.similarUsers.keySet, viewed)
                  case _ =>
                    UserCommandFailed(req.userId, UserStateErrorMessage)
                }

            case req: UserHistoryRetrivalRequest =>
              val viewed: Seq[String] = state.viewed.take(params.numberOfViewsToCheck)
              Effect
                .reply(req.replyTo)(UserHistoryResponse(state.userId, state.similarUsers.keySet, viewed))

            case bookmark: NodeBookmarkRequest =>
              Effect
                .persist(NodeBookmarked(bookmark.userId, bookmark.nodeId, bookmark.tags, System.currentTimeMillis()))
                .thenReply(bookmark.replyTo){
                  case updatedState: CreatedUserState =>
                    NodeBookmarkSuccess(bookmark.userId, bookmark.nodeId, updatedState.labels)
                  case _ =>
                    UserCommandFailed(bookmark.userId, UserStateErrorMessage)
                }

            case removeBookmark: RemoveNodeBookmarkRequest =>
              Effect
                .persist(RemoveNodeBookmarked(removeBookmark.userId, removeBookmark.nodeId))
                .thenReply(removeBookmark.replyTo)(_ => UserCommandSuccess(removeBookmark.userId, state.labels))

            case bookmark: UserBookmarkRequest =>
              val bookmarkEvt = UserBookmarked(bookmark.userId, bookmark.targetUserId, bookmark.labels, bookmark.autoReply, System.currentTimeMillis())
              val bookmarkedByEvt = BookmarkedBy(bookmark.userId, bookmark.targetUserId)
              if(bookmark.autoReply)
                Effect
                  .persist(bookmarkEvt, bookmarkedByEvt).thenReply(bookmark.replyTo){
                    case updatedState: CreatedUserState =>
                      UserBookmarkSuccess(bookmark.userId, updatedState.labels)
                    case _ =>
                      UserCommandFailed(bookmark.userId, UserStateErrorMessage)
                  }
              else
                Effect
                  .persist(bookmarkEvt).thenReply(bookmark.replyTo){
                  case updatedState: CreatedUserState =>
                    UserBookmarkSuccess(bookmark.userId, updatedState.labels)
                  case _ =>
                    UserCommandFailed(bookmark.userId, UserStateErrorMessage)
                }

            case removeUserBookmark: RemoveUserBookmarkRequest =>
              Effect
                .persist(RemoveUserBookmarked(removeUserBookmark.userId, removeUserBookmark.targetUserId))
                .thenReply(removeUserBookmark.replyTo)(_ => UserCommandSuccess(removeUserBookmark.userId, state.labels))

            case bookmark: BookmarkedByRequest =>
              Effect
                .persist(BookmarkedBy(bookmark.userId, bookmark.bookmarkUser, System.currentTimeMillis()))
                .thenReply(bookmark.replyTo)(_ => BookmarkedBySuccess(bookmark.userId, state.labels, state.autoReply))

            case removeBookmarkedBy: RemoveBookmarkedByRequest =>
              Effect
                .persist(RemoveBookmarkedBy(removeBookmarkedBy.userId, removeBookmarkedBy.bookmarkUser))
                .thenReply(removeBookmarkedBy.replyTo)(_ => UserCommandSuccess(removeBookmarkedBy.userId, state.labels))

            case retrieve: UserRetrievalCommand =>
              val similarUser = state.similarUsers.map{
                case (userId, labels) =>
                  userId -> state.labels.foldLeft(0){
                    case (acc, (label, labelWeight)) =>
                      acc + Math.min(labels.getOrElse(label, 0), labelWeight)
                  }
              }.filter(_._2 > params.labelWeightFilter)
          
              Effect.reply(retrieve.replyTo)(UserInfo(state.userId, state.userType, state.properties, state.labels, state.viewed, state.bookmarkedNodes.keySet, state.bookmarkedUsers.keySet, state.bookmarkedBy.keySet, similarUser, state.autoReply))

            case SetAutoReplyCommand(userId, autoReply, replyTo) =>
              Effect.persist(UserAutoReplyUpdate(userId, autoReply)).thenReply(replyTo)(_ => UserCommandSuccess(userId, state.labels))

            case NeighbouringViewsRequest(userId, replyTo) =>
              Effect.reply(replyTo)(NeighbouringViews(userId, state.userType, state.properties, state.labels, state.viewed.take(10)))
          }
      }
  }

  private def eventHandler(context: ActorContext[UserCommand[UserReply]])(implicit params: UserEntityParams): (UserState, UserEvent) => UserState = {
    (state, event) =>
      state match {
        case _: EmptyUserState =>
          event match {
            case created: UserUpdated =>
              CreatedUserState(
                created.userId,
                created.userType,
                created.properties,
                created.labels,
                Map.empty,
                Map.empty,
                Map.empty,
                Map.empty,
                Seq.empty
              )

            case _ =>
              state
          }
        case created: CreatedUserState =>
          event match {
            case update: UserUpdated =>
              created.copy(
                userType = update.userType,
                properties = update.properties,
                labels = update.labels
              )

            case paramsUpdate: ParamsUpdate =>
              created.copy(
                numberOfSimilarUsersOpt = paramsUpdate.numberOfSimilarUsers,
                numberOfViewsToCheckOpt = paramsUpdate.numberOfViewsToCheck,
                labelWeightFilterOpt = paramsUpdate.labelWeightFilter,
                nodeBookmarkBiasOpt = paramsUpdate.nodeBookmarkBias,
                userBookmarkBiasOpt = paramsUpdate.userBookmarkBias,
                nodeVisitBiasOpt = paramsUpdate.nodeVisitBias
              )

            case node: NodeBookmarked =>
              val views = created.viewed.size + 1
              val updatedLabels: Map[String, Int] =
                if(created.bookmarkedNodes.contains(node.nodeId))
                  created.labels
                else
                  node.tags.mapValues(_/views * created.nodeBookmarkBias) ++ created.labels.foldLeft(Map.empty[String, Int]){
                    case (acc, (label, weight)) =>
                      acc + (label -> (weight * views + (node.tags.getOrElse(label, 0) * created.nodeBookmarkBias)) / views)
                  }
              created.copy(
                bookmarkedNodes = created.bookmarkedNodes + (node.nodeId -> node),
                labels = updatedLabels
              )

            case user: UserBookmarked =>
              val bookmarkExists = created.bookmarkedUsers.contains(user.userId)
              val views = created.viewed.size + 1
              val updatedLabels: Map[String, Int] =
                if(bookmarkExists)
                  created.labels
                else
                  user.labels.mapValues(_/views * created.userBookmarkBias) ++ created.labels.foldLeft(Map.empty[String, Int]){
                    case (acc, (label, weight)) =>
                      acc + (label -> (weight * views + (user.labels.getOrElse(label, 0) * created.userBookmarkBias)) / views)
                  }

              val withNewSimilarUsers = created.similarUsers + (user.targetUserId -> user.labels)
              val updatedSimilarUsers =
                if(withNewSimilarUsers.size > created.numberOfSimilarUsers) {
                  val leastSimilarUser: Seq[String] = rankSimilarUsers(withNewSimilarUsers, updatedLabels).drop(created.numberOfSimilarUsers).map(_._1)
                  withNewSimilarUsers -- leastSimilarUser
                } else withNewSimilarUsers

              created.copy(
                bookmarkedUsers = created.bookmarkedUsers + (user.targetUserId -> user),
                labels = updatedLabels.filter(_._2 > created.labelWeightFilter),
                similarUsers = updatedSimilarUsers
              )

            case by: BookmarkedBy =>
              created.copy(bookmarkedBy = created.bookmarkedBy + (by.bookmarkUser -> by))


            case remove: RemoveNodeBookmarked =>
              created.copy(bookmarkedNodes = created.bookmarkedNodes - remove.nodeId)

            case remove: RemoveUserBookmarked =>
              created.copy(bookmarkedUsers = created.bookmarkedUsers - remove.targetUserId)

            case remove: RemoveBookmarkedBy =>
              created.copy(bookmarkedBy = created.bookmarkedBy - remove.bookmarkUser)

            case autoReply: UserAutoReplyUpdate =>
              created.copy(autoReply = autoReply.autoReply)

            case req: UserRequest =>
              val updatedViews = req.nodeId +: created.viewed
              val bias = req.bias match {
                case _: NodeReferralBias =>
                  created.nodeVisitBias
                case _: NodeSearchBias =>
                  created.nodeSearchBias
              }

              val updatedLabels: Map[String, Int] =
                if(created.viewed.contains(req.nodeId))
                  created.labels
                else
                  req.tags.mapValues(_/updatedViews.size * bias) ++ created.labels.foldLeft(Map.empty[String, Int]){
                    case (acc, (label, weight)) =>
                      acc + (label -> (weight * updatedViews.size + (req.tags.getOrElse(label, 0) * bias)) / (updatedViews.size + 1))
                  }

              val withNewSimilarUsers = created.similarUsers ++ req.similarUsers
              val updatedSimilarUsers =
                if(withNewSimilarUsers.size > created.numberOfSimilarUsers) {
                  val leastSimilarUser = rankSimilarUsers(withNewSimilarUsers, updatedLabels).drop(created.numberOfSimilarUsers).map(_._1)
                  withNewSimilarUsers -- leastSimilarUser
                } else withNewSimilarUsers

              created.copy(
                labels = updatedLabels.filter(_._2 > created.labelWeightFilter),
                viewed = updatedViews,
                similarUsers = updatedSimilarUsers
              )

          }
      }
  }

  val TypeKey = EntityTypeKey[UserCommand[UserReply]]("user")
  val UserUpdateTagName = "userupdate"
  val UserEventDefaultTagName = "userevent"

  def userEntityBehaviour(persistenceId: PersistenceId)(implicit params: UserEntityParams): Behavior[UserCommand[UserReply]] = Behaviors.setup { context =>
    EventSourcedBehavior.withEnforcedReplies(
      persistenceId,
      EmptyUserState(),
      commandHandler(context),
      eventHandler(context)
    )
      .withRetention(RetentionCriteria.snapshotEvery(numberOfEvents = 20, keepNSnapshots = 2))
      .withTagger{
        case _: UserUpdated => Set(UserUpdateTagName, UserEventDefaultTagName)
        case _ => Set(UserEventDefaultTagName)
      }
  }
}
