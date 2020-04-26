package com.example.user

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior, ReplyEffect, RetentionCriteria}
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}

object UserNodeEntity {
  type UserId = String
  case class LabelWeight(weight: Int, tag: String)

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[CreateUserCommand], name = "CreateUserCommand"),
      new JsonSubTypes.Type(value = classOf[UpdateUserCommand], name = "UpdateUserCommand"),
      new JsonSubTypes.Type(value = classOf[NodeVisitRequest], name = "NodeVisitRequest")
    )
  )
  sealed trait UserCommand[Reply <: UserReply] {
    val userId: UserId
    def replyTo: ActorRef[Reply]
  }

  case class CreateUserCommand(userId: UserId, userType: String, properties: Map[String, String], labels: Set[LabelWeight], replyTo: ActorRef[UserReply]) extends UserCommand[UserReply]
  case class UpdateUserCommand(userId: UserId, userType: String, properties: Map[String, String], labels: Set[LabelWeight], replyTo: ActorRef[UserReply]) extends UserCommand[UserReply]
  case class NodeVisitRequest(userId: UserId, nodeId: String, tags: Map[String, Int], recommended: Seq[String], relevant: Seq[String], replyTo: ActorRef[UserReply]) extends UserCommand[UserReply]
  case class UserRetrievalCommand(userId: UserId, replyTo: ActorRef[UserReply]) extends UserCommand[UserReply]

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[UserCommandSuccess], name = "UserCommandSuccess"),
      new JsonSubTypes.Type(value = classOf[UserRequestSuccess], name = "UserRequestSuccess"),
      new JsonSubTypes.Type(value = classOf[UserCommandFailed], name = "UserCommandFailed")
    )
  )
  sealed trait UserReply {
    val userId: UserId
  }
  case class UserCommandSuccess(userId: UserId) extends UserReply
  case class UserRequestSuccess(userId: UserId, recommended: Seq[String], relevant: Seq[String]) extends UserReply
  case class UserCommandFailed(userId: UserId, error: String) extends UserReply
  case class UserInfo(userId: UserId, state: CreatedUserState) extends UserReply

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[UserCreated], name = "UserCreated"),
      new JsonSubTypes.Type(value = classOf[UserUpdated], name = "UserUpdated"),
      new JsonSubTypes.Type(value = classOf[UserRequest], name = "UserRequest")
    )
  )
  sealed trait UserEvent
  case class UserCreated(userId: UserId, userType: String, properties: Map[String, String], labels: Set[LabelWeight]) extends UserEvent
  case class UserUpdated(userType: String, properties: Map[String, String], labels: Set[LabelWeight]) extends UserEvent
  case class UserRequest(nodeId: String, tags: Map[String, Int]) extends UserEvent

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
    labels: Set[LabelWeight],
    viewed: Seq[String]
  ) extends UserState

  private def commandHandler(context: ActorContext[UserCommand[UserReply]]): (UserState, UserCommand[UserReply]) => ReplyEffect[UserEvent, UserState] = {
    (state, command) =>
      state match {
        case empty: EmptyUserState =>
          command match {
            case cmd: CreateUserCommand =>
              Effect
                .persist(UserCreated(cmd.userId, cmd.userType, cmd.properties, cmd.labels))
                .thenReply(cmd.replyTo)(_ => UserCommandSuccess(cmd.userId))
            case cmd =>
              Effect.reply(cmd.replyTo)(UserCommandFailed(cmd.userId, "User does not exist"))
          }

        case state: CreatedUserState =>
          command match {
            case update: UpdateUserCommand =>
              Effect
                .persist(UserUpdated(update.userType, update.properties, update.labels))
                .thenReply(update.replyTo)(_ => UserCommandSuccess(update.userId))
            case req: NodeVisitRequest =>
              println(req.tags)
              println(state)
              val toRecommand = req.recommended.filterNot(state.viewed.contains)
              val toRelate = req.relevant.filterNot(state.viewed.contains)
              val evt = UserRequest(req.nodeId, req.tags)
              println(evt)
              Effect
                .persist(evt)
                .thenReply(req.replyTo)(_ => UserRequestSuccess(req.userId, toRecommand, toRelate))
            case cmd: CreateUserCommand =>
              Effect.reply(cmd.replyTo)(UserCommandFailed(cmd.userId, "User already exists"))

            case retrieve: UserRetrievalCommand =>
              println(state)
              Effect.reply(retrieve.replyTo)(UserInfo(state.userId, state))

          }
      }
  }

  private def eventHandler(context: ActorContext[UserCommand[UserReply]]): (UserState, UserEvent) => UserState = {
    (state, event) =>
      state match {
        case _: EmptyUserState =>
          event match {
            case created: UserCreated =>
              CreatedUserState(
                created.userId,
                created.userType,
                created.properties,
                created.labels,
                Seq.empty
              )
          }
        case created: CreatedUserState =>
          event match {
            case _: UserCreated =>
              created
            case update: UserUpdated =>
              CreatedUserState(
                created.userId,
                update.userType,
                update.properties,
                update.labels,
                created.viewed
              )
            case req: UserRequest =>
              val labels = created.labels.foldLeft(Set.empty[LabelWeight]){
                case (acc, labelWeight) =>
                  acc + req.tags.get(labelWeight.tag).map{ weight =>
                    LabelWeight(labelWeight.weight + weight, labelWeight.tag)
                  }.getOrElse(labelWeight)
              }

              val newLabels = req.tags.foldLeft(Set.empty[LabelWeight]){
                case (acc, (tag, weight)) =>
                  if(created.labels.exists(_.tag == tag)) acc
                  else acc + LabelWeight(weight, tag)
              }

              created.copy(
                labels = labels ++ newLabels,
                viewed = req.nodeId +: created.viewed
              )
          }
      }
  }

  val TypeKey = EntityTypeKey[UserCommand[UserReply]]("user")

  def userEntityBehaviour(persistenceId: PersistenceId): Behavior[UserCommand[UserReply]] = Behaviors.setup { context =>
    EventSourcedBehavior.withEnforcedReplies(
      persistenceId,
      EmptyUserState(),
      commandHandler(context),
      eventHandler(context)
    )
      .withRetention(RetentionCriteria.snapshotEvery(numberOfEvents = 20, keepNSnapshots = 2))
  }
}
