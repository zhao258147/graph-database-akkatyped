package com.example.user.http

import com.example.user.UserNodeEntity.{LabelWeight, UserId}

object Requests {
  case class CreateUserReq(userId: UserId, userType: String, properties: Map[String, String] = Map.empty, labels: Set[LabelWeight] = Set.empty)
  case class UpdateUserReq(userId: UserId, userType: String, properties: Map[String, String] = Map.empty, labels: Set[LabelWeight] = Set.empty)
  case class NodeVisitReq(userId: UserId, nodeId: String, tags: Map[String, Int], recommended: Seq[String], relevant: Seq[String], popular: Seq[String])
}
