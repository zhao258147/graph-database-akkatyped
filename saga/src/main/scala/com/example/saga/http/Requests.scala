package com.example.saga.http

import com.example.graph.GraphNodeEntity.{EdgeProperties, NodeId, TargetNodeId}
import com.example.user.UserNodeEntity.UserId

object Requests {
  case class NodeReferralReq(nodeId: NodeId, targetNodeId: TargetNodeId, userId: UserId, userLabels: Set[String])
  case class NodeVisitReq(nodeId: NodeId, targetNodeId: String, edgeType: String, userId: UserId, direction: String = "to", properties: EdgeProperties = Map.empty)
}
