package com.example.user

import akka.actor.typed.ActorRef

object UserNodeEntity {
  type UserId = String

  case class UserRequest(nodeType: String, nodeId: String)

  sealed trait UserNodeCommand[Reply <: UserNodeReply] {
    val userId: UserId
    def replyTo: ActorRef[Reply]
  }

  case class CreateUserCommand(userId: UserId, userType: String, replyTo: ActorRef[UserNodeReply]) extends UserNodeCommand[UserNodeReply]
  case class UserRequestCommand(userId: UserId, request: UserRequest, replyTo: ActorRef[UserNodeReply]) extends UserNodeCommand[UserNodeReply]


  sealed trait UserNodeReply {
    val userId: UserId
  }

  
}
