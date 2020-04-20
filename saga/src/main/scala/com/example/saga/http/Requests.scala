package com.example.saga.http

import com.example.graph.GraphNodeEntity.{EdgeProperties, NodeId, TargetNodeId}
import com.example.user.UserNodeEntity.UserId

object Requests {
  case class NodeReferralReq(nodeId: NodeId, userId: UserId, userLabels: Map[String, Int])
  case class NodeVisitReq(nodeId: NodeId, targetNodeId: String, userId: UserId, direction: String = "to", properties: EdgeProperties = Map.empty, userLabels: Map[String, Int])
}
