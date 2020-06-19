package com.example.user.http

import com.example.user.UserNodeEntity.{UserCommand, UserId, UserReply}

object Requests {
  case class CreateUserReq(userId: UserId, userType: String, properties: Map[String, String] = Map.empty, labels: Map[String, Int] = Map.empty)
  case class UpdateUserReq(userId: UserId, properties: Map[String, String] = Map.empty, labels: Map[String, Int] = Map.empty)
  case class UpdateAutoReplyReq(autoReply: Boolean)

  case class NodeVisitReq(userId: UserId, nodeId: String, tags: Map[String, Int], similarUsers: Map[String, Map[String, Int]])
  case class ListUsersReq(users: Set[String])
}
