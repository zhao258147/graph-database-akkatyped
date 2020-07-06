package com.gotin.analytics.http

import com.example.graph.GraphNodeEntity
import com.example.graph.GraphNodeEntity.{EdgeProperties, NodeId, TargetNodeId}
import com.example.user.UserNodeEntity.UserId

object Requests {
  case class HomePageVisitReq(userId: UserId, userLabels: Map[String, Int])
  case class NodeReferralReq(nodeId: NodeId, userId: UserId, userLabels: Map[String, Int])
  case class NodeVisitReq(nodeId: NodeId, targetNodeId: String, userId: UserId, direction: String = "to", properties: EdgeProperties = Map.empty, userLabels: Map[String, Int])

  case class NodeBookmarkReq(userId: UserId, nodeId: String)
  case class RemoveNodeBookmarkReq(userId: UserId, nodeId: String)

  case class UserBookmarkReq(userId: UserId, targetUserId: String)
  case class RemoveUserBookmarkReq(userId: UserId, targetUserId: String)
}
